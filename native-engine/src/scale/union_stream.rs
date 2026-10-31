//! Streaming UNION ALL preserves input order and checks every batch schema.
use std::hash::{Hash, Hasher};
use crate::{batch::Batch, schema::Schema, Error, Result};
use super::cancel::QueryControl;

pub struct UnionAllStream<L, R> {
    left: L,
    right: R,
    schema: Schema,
    control: QueryControl,
    on_right: bool,
    stopped: bool,
}

impl<L, R> UnionAllStream<L, R> {
    pub fn new(left: L, right: R, schema: Schema, control: QueryControl) -> Self {
        Self { left, right, schema, control, on_right: false, stopped: false }
    }
}

impl<L, R> Iterator for UnionAllStream<L, R>
where L: Iterator<Item = Result<Batch>>, R: Iterator<Item = Result<Batch>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let next = if self.on_right { self.right.next() } else { self.left.next() };
            let Some(next) = next else {
                if !self.on_right {
                    self.on_right = true;
                    continue;
                }
                self.stopped = true;
                return None;
            };
            match next {
                Ok(batch) if batch.schema() == &self.schema && batch.num_rows() == 0 => continue,
                Ok(batch) if batch.schema() == &self.schema => return Some(Ok(batch)),
                Ok(batch) => {
                    // SQL UNION ALL uses column positions, not names. The output
                    // adopts left input names and widened nullability.
                    if batch.schema().len() != self.schema.len() ||
                        !batch.schema().fields().iter().zip(self.schema.fields())
                            .all(|(source, target)| source.data_type == target.data_type) {
                        self.stopped = true;
                        return Some(Err(Error::Schema("UNION ALL input schemas differ".into())));
                    }
                    let rebased = batch.relabel(self.schema.clone());
                    match rebased {
                        Ok(batch) if batch.num_rows() == 0 => continue,
                        Ok(batch) => return Some(Ok(batch)),
                        Err(error) => {
                            self.stopped = true;
                            return Some(Err(error));
                        }
                    }
                }
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn concatenation_stays_bounded_and_ordered() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let one = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap();
        let two = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(2)]]).unwrap();
        let merged = UnionAllStream::new(
            [Ok(one)].into_iter(), [Ok(two)].into_iter(), schema,
            QueryControl::default());
        let rows = merged.collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }

    #[test]
    fn rejects_type_mismatches_without_reading_more_input() {
        let left_schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let right_schema = Schema::new(vec![Field::new("n", DataType::Text, false)]).unwrap();
        let right = Batch::from_rows(right_schema, vec![vec![Value::Text("bad".into())]]).unwrap();
        let mut result = UnionAllStream::new(
            std::iter::empty(), [Ok(right)].into_iter(), left_schema,
            QueryControl::default());
        assert!(result.next().unwrap().is_err());
        assert!(result.next().is_none());
    }
}


/// Bounded UNION DISTINCT across two input streams, preserving first appearance.
pub struct UnionDistinctStream<L, R> {
    inner: UnionAllStream<L, R>,
    seen: std::collections::HashMap<u64, Vec<Vec<crate::scalar::Value>>>,
    unique_keys: usize,
    retained_bytes: usize,
    max_keys: usize,
    max_bytes: usize,
    stopped: bool,
}

impl<L, R> UnionDistinctStream<L, R> {
    pub fn new(
        left: L, right: R, schema: Schema, control: QueryControl,
        max_keys: usize, max_bytes: usize,
    ) -> Result<Self> {
        if max_keys == 0 || max_bytes == 0 {
            return Err(Error::Execution("UNION DISTINCT requires positive memory limits".into()));
        }
        Ok(Self {
            inner: UnionAllStream::new(left, right, schema, control),
            seen: std::collections::HashMap::new(),
            unique_keys: 0, retained_bytes: 0, max_keys, max_bytes, stopped: false,
        })
    }
}

impl<L, R> Iterator for UnionDistinctStream<L, R>
where L: Iterator<Item = Result<Batch>>, R: Iterator<Item = Result<Batch>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            let batch = match self.inner.next()? {
                Ok(batch) => batch,
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            };
            // All zero-column rows are the same key. Keep just the first row,
            // with the same 64-byte key footprint and admission checks.
            if batch.schema().is_empty() {
                if self.unique_keys != 0 { continue; }
                if self.max_keys == 0 || self.max_bytes < 64 {
                    self.stopped = true;
                    return Some(Err(Error::Execution("UNION DISTINCT memory budget exceeded".into())));
                }
                self.unique_keys = 1;
                self.retained_bytes = 64;
                return Some(batch.slice_rows(0, 1));
            }
            // Probe hash buckets using references to column values. Only first
            // occurrences are materialized as owned keys.
            let columns = match (0..batch.schema().len())
                .map(|col| batch.column(col))
                .collect::<Result<Vec<_>>>() {
                    Ok(columns) => columns,
                    Err(error) => {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                };
            let mut selected = Vec::new();
            for row in 0..batch.num_rows() {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                columns.len().hash(&mut hasher);
                for column in &columns { column[row].hash(&mut hasher); }
                let fingerprint = hasher.finish();
                if self.seen.get(&fingerprint).is_some_and(|entries|
                    entries.iter().any(|key| key.iter().zip(&columns)
                        .all(|(value, column)| value == &column[row]))) {
                    continue;
                }
                if self.unique_keys >= self.max_keys {
                    self.stopped = true;
                    return Some(Err(Error::Execution("UNION DISTINCT memory budget exceeded".into())));
                }
                let footprint = super::row_footprint::footprint(
                    columns.iter().map(|column| &column[row])
                );
                let next_total = self.retained_bytes.checked_add(footprint);
                if next_total.is_none_or(|bytes| bytes > self.max_bytes) {
                    self.stopped = true;
                    return Some(Err(Error::Execution("UNION DISTINCT memory budget exceeded".into())));
                }
                let key = match batch.row(row) {
                    Ok(key) => key,
                    Err(error) => {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                };
                self.retained_bytes = next_total.expect("validated set memory");
                self.unique_keys += 1;
                self.seen.entry(fingerprint).or_default().push(key);
                selected.push(row);
            }
            if !selected.is_empty() {
                if selected.len() == batch.num_rows() {
                    return Some(Ok(batch));
                }
                return Some(batch.take(&selected));
            }
        }
    }
}

#[cfg(test)]
mod union_distinct_stream_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn stream_set_union_preserves_order_and_deduplicates_nulls() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Null]
        ]).unwrap();
        let union = UnionDistinctStream::new(
            [Ok(a)].into_iter(), [Ok(b)].into_iter(), schema,
            QueryControl::default(), 10, 4096
        ).unwrap();
        let rows = union.collect::<Result<Vec<_>>>().unwrap()
            .iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)]
        ]);
    }

    #[test]
    fn exceeding_key_budget_is_a_terminal_error() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let mut union = UnionDistinctStream::new(
            [Ok(batch)].into_iter(), std::iter::empty(), schema,
            QueryControl::default(), 1, 4096
        ).unwrap();
        assert!(union.next().unwrap().is_err());
        assert!(union.next().is_none());
    }
}


#[cfg(test)]
mod union_positional_schema_engine_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn renames_right_columns_to_the_union_output_schema() {
        let a = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let b = Schema::new(vec![Field::new("alias", DataType::Integer, true)]).unwrap();
        let output_schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(a, vec![vec![Value::Integer(1)]]).unwrap();
        let second = Batch::from_rows(b, vec![vec![Value::Null]]).unwrap();
        let results = UnionAllStream::new(
            [Ok(first)].into_iter(), [Ok(second)].into_iter(),
            output_schema.clone(), QueryControl::default(),
        ).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|part| part.schema() == &output_schema));
        assert_eq!(results[0].row(0).unwrap(), vec![Value::Integer(1)]);
        assert_eq!(results[1].row(0).unwrap(), vec![Value::Null]);
    }
}

#[cfg(test)]
mod consolidated_empty_union_skip_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn union_all_skips_zero_row_batches_but_keeps_values() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let empty = Batch::empty(schema.clone());
        let one = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(5)]]).unwrap();
        let batches = UnionAllStream::new([Ok(empty)].into_iter(), [Ok(one)].into_iter(),
            schema, QueryControl::default()).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row(0).unwrap(), vec![Value::Integer(5)]);
    }
}

#[cfg(test)]
mod union_distinct_single_probe_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn duplicate_keys_remain_allowed_at_exact_capacity() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, false)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let batches = UnionDistinctStream::new([Ok(a)].into_iter(), [Ok(b)].into_iter(),
            schema, QueryControl::default(), 2, 8192).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.into_iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }
}

#[cfg(test)]
mod union_schema_move_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn right_side_aliasing_and_nullability_preserve_values() {
        let output = Schema::new(vec![Field::new("unified", DataType::Text, true)]).unwrap();
        let right_schema = Schema::new(vec![Field::new("other", DataType::Text, false)]).unwrap();
        let right = Batch::from_rows(right_schema, vec![
            vec![Value::Text("large-value".repeat(100))],
            vec![Value::Text("another".into())],
        ]).unwrap();
        let output_stream = UnionAllStream::new(
            std::iter::empty(), [Ok(right)].into_iter(), output.clone(),
            QueryControl::default(),
        );
        let batches = output_stream.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), &output);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].value(0, 0).unwrap(),
            &Value::Text("large-value".repeat(100)));
    }
}

#[cfg(test)]
mod union_borrowed_row_probe_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn handles_repeated_large_keys_across_streams_and_nulls() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Text, true),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let big = Value::Text("repeated-unicode-é".repeat(512));
        let left = Batch::from_rows(schema.clone(), vec![
            vec![big.clone(), Value::Integer(1)],
            vec![big.clone(), Value::Integer(1)],
            vec![Value::Null, Value::Integer(0)],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![big.clone(), Value::Integer(1)],
            vec![big, Value::Integer(2)],
            vec![Value::Null, Value::Integer(0)],
        ]).unwrap();
        let output = UnionDistinctStream::new([Ok(left)].into_iter(), [Ok(right)].into_iter(),
            schema, QueryControl::default(), 3, 32 * 1024).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.into_iter().flat_map(|b| b.rows().unwrap()).count(), 3);
    }
}

#[cfg(test)]
mod zero_column_stream_union_fast_path_tests {
    use super::*;
    #[test]
    fn emits_one_zero_width_row_across_large_stream_batches() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 100_000).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 200_000).unwrap();
        let output = UnionDistinctStream::new(
            [Ok(left)].into_iter(), [Ok(right)].into_iter(),
            schema.clone(), QueryControl::default(), 1, 64).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).sum::<usize>(), 1);
        let left = Batch::new_with_row_count(schema.clone(), vec![], 1).unwrap();
        let mut limited = UnionDistinctStream::new([Ok(left)].into_iter(),
            std::iter::empty(), schema, QueryControl::default(), 0, 64).unwrap();
        assert!(limited.next().unwrap().is_err());
        assert!(limited.next().is_none());
    }
}
