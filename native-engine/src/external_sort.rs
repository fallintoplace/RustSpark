//! External merge sort over Arrow IPC runs, independent from DataFusion.
use std::{cmp::Ordering, path::Path, sync::OnceLock};

use crate::{
    batch::Batch,
    scalar::Value,
    sort::{self, SortKey},
    spill_ipc::{self, SpillFile, SpillReader},
    schema::Schema,
    Error, Result,
};

const MAX_OUTPUT_BATCH_BYTES: usize = 16 * 1024 * 1024;

// Reserve at most the fixed Value slots that the output budget can hold.
// This avoids allocating columns * 256 slots for very wide result schemas.
fn output_buffer_capacity(requested: usize, columns: usize) -> usize {
    let fixed_row = columns.max(1).saturating_mul(std::mem::size_of::<Value>());
    requested.min(256).min((MAX_OUTPUT_BATCH_BYTES / fixed_row.max(1)).max(1))
}

struct Run {
    _spill: SpillFile,
    reader: SpillReader,
    batch: Option<Batch>,
    position: usize,
    // Cache each computed expression only after a comparator needs it.
    // A consumed row clears the cells before the next comparison.
    computed: Vec<OnceLock<Result<crate::scalar::Value>>>,
}

// Resolve direct column references once per sort, not on every tournament
// comparison. Computed expressions retain the existing evaluator path.
struct BoundSortKey {
    key: SortKey,
    column: Option<usize>,
}

pub struct ExternalSort {
    schema: Schema,
    keys: Vec<BoundSortKey>,
    runs: Vec<Run>,
    winners: RunTournament,
    output_rows: usize,
    pending_error: Option<Error>,
    finished: bool,
}

/// Sort incoming batches into disk runs and merge them lazily.
/// Each run holds at most run_rows, and output is produced in output_rows
/// batches rather than being collected into one in-memory table.
pub fn external_sort(
    input: impl IntoIterator<Item = Batch>,
    schema: &Schema,
    keys: &[SortKey],
    spill_dir: impl AsRef<Path>,
    run_rows: usize,
    output_rows: usize,
) -> Result<ExternalSort> {
    if run_rows == 0 || output_rows == 0 || run_rows > 65_536 || output_rows > 65_536 {
        return Err(Error::Execution("external sort batch sizes must be in 1..=65536".into()));
    }
    let bound_keys = keys.iter().map(|key| {
        let column = match &key.expression {
            crate::expr::Expr::Column(name) => Some(schema.column(name)?),
            _ => None,
        };
        Ok(BoundSortKey { key: key.clone(), column })
    }).collect::<Result<Vec<_>>>()?;
    let mut runs = Vec::new();
    for input_batch in input {
        if input_batch.schema() != schema {
            return Err(Error::Schema("external sort schema mismatch".into()));
        }
        // Move each owned column through its run windows. Borrowed slicing
        // cloned every TEXT payload before sorting and writing the spill.
        let rows = input_batch.num_rows();
        let mut columns = input_batch.into_columns().into_iter()
            .map(Vec::into_iter).collect::<Vec<_>>();
        for start in (0..rows).step_by(run_rows) {
            if runs.len() >= 256 {
                return Err(Error::Execution("too many external sort runs".into()));
            }
            let take = (rows - start).min(run_rows);
            let window = columns.iter_mut()
                .map(|column| column.by_ref().take(take).collect()).collect();
            let batch = Batch::from_validated_columns(schema.clone(), window, take)?;
            let sorted = sort::sort_owned(batch, keys)?;
            let spill = spill_ipc::spill(spill_dir.as_ref(), schema, &[sorted])?;
            let mut reader = spill.read()?;
            let first = reader.next().transpose()?;
            runs.push(Run {
                _spill: spill, reader, batch: first, position: 0,
                computed: std::iter::repeat_with(OnceLock::new)
                    .take(bound_keys.len()).collect(),
            });
        }
    }
    // Build a fallible tournament tree once, then update only the path
    // affected by each emitted row. This avoids comparing every run per row.
    let winners = RunTournament::new(&runs, &bound_keys)?;
    Ok(ExternalSort {
        schema: schema.clone(),
        keys: bound_keys,
        runs,
        winners,
        output_rows,
        pending_error: None,
        finished: false,
    })
}

fn compare_runs(left: &Run, right: &Run, keys: &[BoundSortKey]) -> Result<Ordering> {
    let a = left.batch.as_ref().ok_or_else(|| Error::Execution("empty sort run".into()))?;
    let b = right.batch.as_ref().ok_or_else(|| Error::Execution("empty sort run".into()))?;
    for (index, key) in keys.iter().enumerate() {
        let x = match key.column {
            Some(column) => a.value(left.position, column)?,
            None => left.computed[index]
                .get_or_init(|| key.key.expression.evaluate(a, left.position))
                .as_ref().map_err(Clone::clone)?,
        };
        let y = match key.column {
            Some(column) => b.value(right.position, column)?,
            None => right.computed[index]
                .get_or_init(|| key.key.expression.evaluate(b, right.position))
                .as_ref().map_err(Clone::clone)?,
        };
        let order = crate::sort::compare_values(x, y, key.key.descending, key.key.nulls_first);
        if order != Ordering::Equal { return Ok(order); }
    }
    Ok(Ordering::Equal)
}

struct RunTournament {
    tree: Vec<Option<usize>>,
    leaves: usize,
}

impl RunTournament {
    fn select(runs: &[Run], keys: &[BoundSortKey], a: Option<usize>, b: Option<usize>) -> Result<Option<usize>> {
        match (a, b) {
            (None, other) | (other, None) => Ok(other),
            (Some(left), Some(right)) => {
                let order = compare_runs(&runs[left], &runs[right], keys)?;
                // Earlier input runs win equal-key ties, retaining stable order.
                Ok(Some(if order == Ordering::Greater { right } else { left }))
            }
        }
    }

    fn new(runs: &[Run], keys: &[BoundSortKey]) -> Result<Self> {
        let leaves = runs.len().next_power_of_two().max(1);
        let mut tree = vec![None; leaves * 2];
        for (index, run) in runs.iter().enumerate() {
            if run.batch.is_some() { tree[leaves + index] = Some(index); }
        }
        for node in (1..leaves).rev() {
            tree[node] = Self::select(runs, keys, tree[node * 2], tree[node * 2 + 1])?;
        }
        Ok(Self { tree, leaves })
    }

    fn winner(&self) -> Option<usize> { self.tree[1] }

    fn update(&mut self, index: usize, runs: &[Run], keys: &[BoundSortKey]) -> Result<()> {
        let mut node = self.leaves + index;
        self.tree[node] = runs[index].batch.as_ref().map(|_| index);
        while node > 1 {
            node /= 2;
            self.tree[node] = Self::select(runs, keys, self.tree[node * 2], self.tree[node * 2 + 1])?;
        }
        Ok(())
    }
}

fn retained_row_bytes(run: &Run, schema: &Schema) -> Result<usize> {
    let batch = run.batch.as_ref()
        .ok_or_else(|| Error::Execution("missing active external sort batch".into()))?;
    let mut bytes = schema.len().saturating_mul(std::mem::size_of::<Value>());
    for column in 0..schema.len() {
        if let Value::Text(text) = batch.value(run.position, column)? {
            bytes = bytes.saturating_add(text.len());
        }
    }
    Ok(bytes)
}

impl Iterator for ExternalSort {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        if let Some(error) = self.pending_error.take() {
            self.finished = true;
            return Some(Err(error));
        }
        // Do not reserve output_rows slots for every column when no runs
        // remain. A wide schema with a large output batch can otherwise
        // allocate substantial memory just to return end-of-stream.
        if self.winners.winner().is_none() {
            self.finished = true;
            return None;
        }
        let capacity = output_buffer_capacity(self.output_rows, self.schema.len());
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(capacity)).collect::<Vec<_>>();
        let mut row_count = 0usize;
        let mut output_bytes = 0usize;
        while row_count < self.output_rows {
            let Some(selected) = self.winners.winner() else {
                self.finished = true;
                break;
            };
            // A row may contain large TEXT even when the requested row
            // count is small. Enforce retained output bytes before cloning.
            let bytes = match retained_row_bytes(&self.runs[selected], &self.schema) {
                Ok(bytes) if bytes <= MAX_OUTPUT_BATCH_BYTES => bytes,
                Ok(_) => {
                    let error = Error::Execution("external sort row exceeds output byte budget".into());
                    if row_count == 0 {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
                Err(error) => {
                    if row_count == 0 {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
            };
            if row_count != 0 && output_bytes.saturating_add(bytes) > MAX_OUTPUT_BATCH_BYTES {
                // Leave this winner and its cursor untouched for the next batch.
                break;
            }
            let run = &mut self.runs[selected];
            let Some(batch) = run.batch.as_ref() else {
                let error = Error::Execution("missing active external sort batch".into());
                if row_count == 0 {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.pending_error = Some(error);
                break;
            };
            if let Err(error) = batch.append_row_to_columns(run.position, &mut columns) {
                if row_count == 0 {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.pending_error = Some(error);
                break;
            }
            output_bytes += bytes;
            row_count += 1;
            run.position += 1;
            if run.position == batch.num_rows() {
                match run.reader.next().transpose() {
                    Ok(next) => {
                        run.batch = next;
                        run.position = 0;
                    }
                    Err(error) => {
                        self.pending_error = Some(error);
                        break;
                    }
                }
            }
            // This run's position has moved. Never reuse previous row keys.
            for cached in &mut run.computed { cached.take(); }
            if let Err(error) = self.winners.update(selected, &self.runs, &self.keys) {
                self.pending_error = Some(error);
                break;
            }
        }
        if row_count == 0 {
            None
        } else {
            // Every source cell came from a validated IPC batch. The output
            // only needs shape validation, not a second scalar type scan.
            Some(Batch::from_validated_columns(self.schema.clone(), columns, row_count))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::Expr,
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn rejects_excessive_batch_allocations_before_creating_files() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let dir = std::env::temp_dir();
        assert!(external_sort(Vec::<Batch>::new(), &schema, &[], &dir, 65_537, 1).is_err());
        assert!(external_sort(Vec::<Batch>::new(), &schema, &[], &dir, 1, 65_537).is_err());
    }

    #[test]
    fn merge_runs_matches_single_batch_sort() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(10)], vec![Value::Integer(2)],
            vec![Value::Null], vec![Value::Integer(1)],
            vec![Value::Integer(5)], vec![Value::Integer(3)],
        ]).unwrap();
        let keys = vec![SortKey::ascending(Expr::Column("key".into()))];
        let expected = sort::sort(&input, &keys).unwrap().rows().unwrap();
        let merged = external_sort(
            vec![input], &schema, &keys, std::env::temp_dir(), 2, 3
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(merged.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>(), expected);
    }
}

#[cfg(test)]
mod output_allocation_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn empty_input_still_enforces_output_batch_budget() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        assert!(external_sort(Vec::<Batch>::new(), &schema, &[],
            std::env::temp_dir(), 1, usize::MAX).is_err());
    }
}

#[cfg(test)]
mod external_sort_borrowed_column_keys_tests {
    use super::*;
    use crate::{
        expr::Expr, scalar::Value, schema::{DataType, Field, Schema},
    };

    #[test]
    fn column_key_merges_preserve_wide_text_nulls_and_stable_ties() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("payload", DataType::Integer, false),
        ]).unwrap();
        let prefix = "é🙂".repeat(1024);
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(format!("{prefix}c")), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Text(format!("{prefix}a")), Value::Integer(3)],
            vec![Value::Text(format!("{prefix}b")), Value::Integer(4)],
        ]).unwrap();
        let keys = vec![SortKey::ascending(Expr::Column("key".into()))];
        let result = external_sort(vec![batch.clone()], &schema, &keys,
            std::env::temp_dir(), 2, 2).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let merged = result.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        let expected = sort::sort(&batch, &keys).unwrap().rows().unwrap();
        assert_eq!(merged, expected);
    }
}

#[cfg(test)]
mod tournament_merge_regressions {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn hundreds_of_runs_match_stable_sort_for_mixed_keys() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("seq", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..233).rev().map(|n| vec![
            if n % 17 == 0 { Value::Null } else { Value::Integer(n % 11) },
            Value::Integer(n),
        ]).collect()).unwrap();
        for keys in [
            vec![SortKey::ascending(Expr::Column("key".into()))],
            vec![SortKey {
                expression: Expr::Column("key".into()),
                descending: true, nulls_first: false,
            }],
            vec![
                SortKey::ascending(Expr::Column("key".into())),
                SortKey { expression: Expr::Column("seq".into()),
                    descending: true, nulls_first: true },
            ],
        ] {
            let expected = sort::sort(&batch, &keys).unwrap().rows().unwrap();
            for run_rows in [1, 3, 11] {
                let result = external_sort(vec![batch.clone()], &schema, &keys,
                    std::env::temp_dir(), run_rows, 7).unwrap()
                    .collect::<Result<Vec<_>>>().unwrap();
                let actual = result.into_iter().flat_map(|chunk| chunk.rows().unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "run_rows={run_rows}");
            }
        }
    }

    #[test]
    fn empty_merge_has_no_selected_run() {
        let schema = Schema::new(vec![]).unwrap();
        let mut sorted = external_sort(vec![], &schema, &[],
            std::env::temp_dir(), 1, 1).unwrap();
        assert_eq!(sorted.winners.winner(), None);
        assert!(sorted.next().is_none());
        assert!(sorted.next().is_none());
    }
}

#[cfg(test)]
mod bound_external_sort_keys_tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn bound_column_and_computed_keys_match_full_sort_across_runs() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Integer, false),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..70).map(|id| vec![
            Value::Integer((id * 19) % 13), Value::Integer(id),
        ]).collect()).unwrap();
        let keys = [
            SortKey::ascending(Expr::Column("x".into())),
            SortKey::ascending(Expr::binary(
                Expr::Column("id".into()), BinaryOp::Add, Expr::Literal(Value::Integer(1)),
            )),
        ];
        let expected = sort::sort(&batch, &keys).unwrap().rows().unwrap();
        let actual = external_sort(vec![batch], &schema, &keys,
            std::env::temp_dir(), 3, 7).unwrap()
            .collect::<Result<Vec<_>>>().unwrap().into_iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod columnar_external_merge_output_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn many_small_runs_keep_nullable_text_and_stable_ties() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let batches = (0..5).map(|part| Batch::from_rows(schema.clone(),
            (0..11).map(|row| vec![
                if row % 9 == 0 { Value::Null } else { Value::Integer((row % 4) as i64) },
                if row % 7 == 0 { Value::Null } else {
                    Value::Text(format!("{part}:{row}:{}", "é🙂".repeat(128)))
                },
            ]).collect()).unwrap()).collect::<Vec<_>>();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let combined = crate::batch_chunks::concatenate(&batches).unwrap();
        let expected = sort::sort(&combined, &keys).unwrap().rows().unwrap();
        let actual = external_sort(batches, &schema, &keys, std::env::temp_dir(), 3, 7)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual.into_iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), expected);
    }
}

#[cfg(test)]
mod deferred_external_sort_error_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, scalar::Value, schema::{DataType, Field}};

    #[test]
    fn computed_key_failure_preserves_prior_output_rows() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("d", DataType::Integer, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(0)],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2), Value::Integer(1)],
        ]).unwrap();
        let mut runs = Vec::new();
        for batch in [left, right] {
            let spill = spill_ipc::spill(std::env::temp_dir(), &schema, &[batch]).unwrap();
            let mut reader = spill.read().unwrap();
            let first = reader.next().unwrap().unwrap();
            runs.push(Run {
                _spill: spill, reader, batch: Some(first), position: 0,
                computed: std::iter::repeat_with(OnceLock::new).take(1).collect(),
            });
        }
        let keys = vec![BoundSortKey {
            key: SortKey::ascending(Expr::binary(
                Expr::Column("n".into()), BinaryOp::Divide,
                Expr::Column("d".into()),
            )),
            column: None,
        }];
        let winners = RunTournament::new(&runs, &keys).unwrap();
        let mut merge = ExternalSort {
            schema, keys, runs, winners, output_rows: 16,
            pending_error: None, finished: false,
        };
        let prefix = merge.next().unwrap().unwrap();
        assert_eq!(prefix.rows().unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)]
        ]);
        assert!(merge.next().unwrap().is_err());
        assert!(merge.next().is_none());
    }
}

#[cfg(test)]
mod lazy_external_sort_expression_cache_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn computed_keys_match_stable_full_sort_across_many_runs_and_batches() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, false),
        ]).unwrap();
        let batches = (0..8).map(|partition| {
            Batch::from_rows(schema.clone(), (0..9).map(|row| {
                let id = ((partition * 37 + row * 17) % 31) as i64;
                vec![Value::Integer(id), Value::Text(format!("{partition}:{row}"))]
            }).collect()).unwrap()
        }).collect::<Vec<_>>();
        let keys = [
            SortKey::ascending(Expr::binary(
                Expr::Column("id".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)),
            )),
            SortKey::ascending(Expr::Column("label".into())),
        ];
        let combined = crate::batch_chunks::concatenate(&batches).unwrap();
        let expected = sort::sort(&combined, &keys).unwrap().rows().unwrap();
        let output = external_sort(batches, &schema, &keys, std::env::temp_dir(), 3, 7)
            .unwrap().collect::<Result<Vec<_>>>().unwrap().into_iter()
            .flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(output, expected);
    }
}

#[cfg(test)]
mod empty_external_merge_allocation_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn empty_wide_schema_finishes_before_reserving_output_columns() {
        let schema = Schema::new((0..64)
            .map(|index| Field::new(format!("column_{index}"), DataType::Text, true))
            .collect()).unwrap();
        let mut sorted = external_sort(
            Vec::<Batch>::new(), &schema, &[], std::env::temp_dir(), 64, 65_536,
        ).unwrap();
        assert!(sorted.next().is_none());
        assert!(sorted.finished);
        assert!(sorted.next().is_none());
    }
}
#[cfg(test)]
mod external_sort_output_byte_budget_regressions {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field}};

    #[test]
    fn wide_text_rows_are_emitted_in_separate_bounded_batches() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let large = 9 * 1024 * 1024;
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("z".repeat(large))],
            vec![Value::Text("a".repeat(large))],
        ]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("text".into()))];
        let mut sorted = external_sort([input], &schema, &keys,
            std::env::temp_dir(), 1, 100).unwrap();
        let first = sorted.next().unwrap().unwrap();
        let second = sorted.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(second.num_rows(), 1);
        assert!(matches!(first.value(0, 0).unwrap(), Value::Text(text)
            if text.len() == large && text.starts_with('a')));
        assert!(matches!(second.value(0, 0).unwrap(), Value::Text(text)
            if text.len() == large && text.starts_with('z')));
        assert!(sorted.next().is_none());
    }
}


#[cfg(test)]
mod external_sort_wide_output_reservation_tests {
    use super::*;

    #[test]
    fn initial_buffers_never_reserve_more_than_output_value_budget() {
        for width in [1, 2, 16, 1024, 4096, 65_536] {
            let capacity = output_buffer_capacity(65_536, width);
            assert!((1..=256).contains(&capacity));
            let fixed_bytes = width * capacity * std::mem::size_of::<Value>();
            assert!(fixed_bytes <= MAX_OUTPUT_BATCH_BYTES);
        }
        assert_eq!(output_buffer_capacity(3, 256), 3);
    }

    #[test]
    fn narrow_outputs_keep_the_small_batch_fast_path() {
        assert_eq!(output_buffer_capacity(65_536, 1), 256);
        assert_eq!(output_buffer_capacity(5, 1), 5);
        assert_eq!(output_buffer_capacity(1, 4096), 1);
    }
}

#[cfg(test)]
mod owned_external_sort_run_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn moves_wide_nullable_text_across_stable_run_windows() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let source = (0..39).rev().map(|index| vec![
            if index % 13 == 0 { Value::Null } else { Value::Integer(index % 5) },
            if index % 11 == 0 { Value::Null } else {
                Value::Text(format!("{index}-{}", "🙂".repeat(4_096)))
            },
        ]).collect::<Vec<_>>();
        let batch = Batch::from_rows(schema.clone(), source).unwrap();
        let keys = [SortKey::ascending(crate::expr::Expr::Column("key".into()))];
        let expected = sort::sort(&batch, &keys).unwrap().rows().unwrap();
        let result = external_sort([batch], &schema, &keys, std::env::temp_dir(), 5, 7)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert!(result.iter().all(|part| part.num_rows() <= 7));
        assert_eq!(
            result.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>(),
            expected,
        );
    }

    #[test]
    fn empty_typed_batches_create_no_spill_files() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = [Batch::empty(schema.clone()), Batch::empty(schema.clone())];
        let keys = [SortKey::ascending(crate::expr::Expr::Column("n".into()))];
        assert!(external_sort(input, &schema, &keys, std::env::temp_dir(), 3, 2)
            .unwrap().next().is_none());
    }
}
