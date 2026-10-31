//! External merge sort over Arrow IPC runs, independent from DataFusion.
use std::{borrow::Cow, cmp::Ordering, path::Path};

use crate::{
    batch::Batch,
    sort::{self, SortKey},
    spill_ipc::{self, SpillFile, SpillReader},
    schema::Schema,
    Error, Result,
};

struct Run {
    _spill: SpillFile,
    reader: SpillReader,
    batch: Option<Batch>,
    position: usize,
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
        for start in (0..input_batch.num_rows()).step_by(run_rows) {
            if runs.len() >= 256 {
                return Err(Error::Execution("too many external sort runs".into()));
            }
            let end = start.saturating_add(run_rows).min(input_batch.num_rows());
            let batch = input_batch.slice_rows(start, end)?;
            let sorted = sort::sort(&batch, keys)?;
            let spill = spill_ipc::spill(spill_dir.as_ref(), schema, &[sorted])?;
            let mut reader = spill.read()?;
            let first = reader.next().transpose()?;
            runs.push(Run { _spill: spill, reader, batch: first, position: 0 });
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
    for key in keys {
        let x: Cow<'_, crate::scalar::Value> = match key.column {
            Some(index) => Cow::Borrowed(a.value(left.position, index)?),
            None => Cow::Owned(key.key.expression.evaluate(a, left.position)?),
        };
        let y: Cow<'_, crate::scalar::Value> = match key.column {
            Some(index) => Cow::Borrowed(b.value(right.position, index)?),
            None => Cow::Owned(key.key.expression.evaluate(b, right.position)?),
        };
        let order = crate::sort::compare_values(&x, &y, key.key.descending, key.key.nulls_first);
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

impl Iterator for ExternalSort {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        if let Some(error) = self.pending_error.take() {
            self.finished = true;
            return Some(Err(error));
        }
        let mut columns = (0..self.schema.len())
            .map(|_| Vec::with_capacity(self.output_rows)).collect::<Vec<_>>();
        let mut row_count = 0usize;
        while row_count < self.output_rows {
            let Some(selected) = self.winners.winner() else {
                self.finished = true;
                break;
            };
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
            runs.push(Run { _spill: spill, reader, batch: Some(first), position: 0 });
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
