//! Bounded native streaming INTERSECT and EXCEPT with SQL bag semantics.
//! Right-side distinct keys are materialized once, while left batches stay streaming.
use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    batch::Batch, scalar::Value, schema::{DataType, Field, Schema}, Error, Result,
};
use super::{cancel::QueryControl, stream_execute::BatchStream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetMode { IntersectDistinct, IntersectAll, ExceptDistinct, ExceptAll }

/// Match the in-memory set operator's numeric widening and SQL NULL semantics.
pub fn set_schema(left: &Schema, right: &Schema) -> Result<Schema> {
    if left.len() != right.len() {
        return Err(Error::Schema("set operation column counts must match".into()));
    }
    let fields = left.fields().iter().zip(right.fields()).map(|(a, b)| {
        let kind = match (a.data_type, b.data_type) {
            (lhs, rhs) if lhs == rhs => lhs,
            (DataType::Integer, DataType::Float) |
            (DataType::Float, DataType::Integer) => DataType::Float,
            _ => return Err(Error::Type(format!("set operation type mismatch at {}", a.name))),
        };
        Ok(Field::new(&a.name, kind, a.nullable || b.nullable))
    }).collect::<Result<Vec<_>>>()?;
    Schema::new(fields)
}

fn convert_row(batch: &Batch, index: usize, schema: &Schema) -> Result<Vec<Value>> {
    let mut values = batch.row(index)?;
    for (value, (input, output)) in values.iter_mut()
        .zip(batch.schema().fields().iter().zip(schema.fields())) {
        if input.data_type == DataType::Integer && output.data_type == DataType::Float {
            if let Value::Integer(number) = value {
                *value = Value::Float(*number as f64);
            }
        }
    }
    Ok(values)
}

pub struct SetStream {
    left: BatchStream,
    left_schema: Schema,
    output_schema: Schema,
    right_counts: HashMap<Vec<Value>, usize>,
    emitted: HashSet<Vec<Value>>,
    mode: SetMode,
    control: QueryControl,
    max_keys: usize,
    max_bytes: usize,
    retained_bytes: usize,
    pending_error: Option<Error>,
    stopped: bool,
}

impl SetStream {
    pub fn new(
        left: BatchStream, right: BatchStream,
        left_schema: Schema, right_schema: Schema,
        mode: SetMode, control: QueryControl,
        max_keys: usize, max_bytes: usize,
    ) -> Result<Self> {
        let output_schema = set_schema(&left_schema, &right_schema)?;
        let mut right_counts: HashMap<Vec<Value>, usize> = HashMap::new();
        let track_multiplicity = matches!(mode, SetMode::IntersectAll | SetMode::ExceptAll);
        let borrow_right = right_schema.len() == 1 &&
            right_schema.fields()[0].data_type == output_schema.fields()[0].data_type;
        let mut retained_bytes = 0usize;
        // Bounded materialization of right-hand distinct keys, not its row count.
        for batch in right {
            control.check()?;
            let batch = batch?;
            if batch.schema() != &right_schema {
                return Err(Error::Schema("stream set right schema changed".into()));
            }
            // DISTINCT modes do not track multiplicity. Adjacent duplicate
            // composite keys need neither row conversion nor a hash probe.
            // ALL modes still update the bag count for every input row.
            let distinct_columns = if track_multiplicity { None } else {
                Some((0..batch.schema().len()).map(|column| batch.column(column))
                    .collect::<Result<Vec<_>>>()?)
            };
            let mut previous_row = None;
            for row in 0..batch.num_rows() {
                if row % 1024 == 0 { control.check()?; }
                if distinct_columns.as_ref().is_some_and(|columns|
                    previous_row.is_some_and(|previous|
                        columns.iter().all(|column| column[previous] == column[row]))) {
                    continue;
                }
                previous_row = Some(row);
                // For common single-column sets, probe an existing key with
                // the borrowed cell. Duplicate TEXT rows need no temporary
                // Vec or cloned String on the right-hand build side.
                if borrow_right {
                    let value = &batch.column(0)?[row];
                    if let Some(count) = right_counts.get_mut(std::slice::from_ref(value)) {
                        if track_multiplicity {
                            *count = count.checked_add(1)
                                .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
                        }
                        continue;
                    }
                }
                let values = convert_row(&batch, row, &output_schema)?;
                let at_limit = right_counts.len() >= max_keys;
                match right_counts.entry(values) {
                    Entry::Occupied(mut entry) => {
                        // DISTINCT modes need membership only. Repeated right-side
                        // keys do not require a counter update or overflow check.
                        if track_multiplicity {
                            let next = entry.get().checked_add(1)
                                .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
                            *entry.get_mut() = next;
                        }
                    }
                    Entry::Vacant(entry) => {
                        let bytes = super::row_footprint::footprint(entry.key().iter());
                        retained_bytes = retained_bytes.checked_add(bytes)
                            .filter(|size| *size <= max_bytes && !at_limit)
                            .ok_or_else(|| Error::Execution("stream set memory budget exceeded".into()))?;
                        entry.insert(1);
                    }
                }
            }
        }
        Ok(Self {
            left, left_schema, output_schema, right_counts, emitted: HashSet::new(),
            mode, control, max_keys, max_bytes, retained_bytes,
            pending_error: None, stopped: false,
        })
    }

    // An exhausted bag key can no longer suppress or produce a left row.
    // Release the actual owned key capacity, never the probing row's capacity.
    fn remove_exhausted_right_key(&mut self, row: &[Value]) {
        if self.right_counts.get(row) != Some(&0) { return; }
        if let Some((owned, _)) = self.right_counts.remove_entry(row) {
            self.retained_bytes -= super::row_footprint::footprint(owned.iter());
        }
    }

    fn select_row(&mut self, row: &[Value]) -> Result<bool> {
        let selected = match self.mode {
            SetMode::IntersectAll => {
                let matched = if let Some(remaining) = self.right_counts.get_mut(row) {
                    *remaining -= 1;
                    true
                } else { false };
                if matched { self.remove_exhausted_right_key(row); }
                matched
            }
            SetMode::ExceptAll => {
                let suppressed = if let Some(remaining) = self.right_counts.get_mut(row) {
                    *remaining -= 1;
                    true
                } else { false };
                if suppressed { self.remove_exhausted_right_key(row); }
                !suppressed
            }
            SetMode::IntersectDistinct => {
                // Right-hand membership already holds every qualifying key.
                // Remove a match on first use instead of keeping a second
                // key copy in the emitted set and charging the budget twice.
                if let Some((stored_key, _)) = self.right_counts.remove_entry(row) {
                    // Charge and release the same stored allocation. An equal
                    // incoming TEXT value can have a very different capacity.
                    let bytes = super::row_footprint::footprint(stored_key.iter());
                    self.retained_bytes -= bytes;
                    true
                } else {
                    false
                }
            }
            SetMode::ExceptDistinct => {
                if self.right_counts.contains_key(row) || self.emitted.contains(row) {
                    false
                } else {
                    if self.emitted.len().saturating_add(self.right_counts.len()) >= self.max_keys {
                        return Err(Error::Execution("stream set memory budget exceeded".into()));
                    }
                    // The retained key is a clone. Input TEXT capacity can
                    // be much larger than the clone's allocation. First
                    // reject impossible values without cloning the row.
                    let minimum = row.iter().fold(
                        64usize.saturating_add(row.len().saturating_mul(
                            std::mem::size_of::<Value>())),
                        |bytes, value| match value {
                            Value::Text(text) => bytes.saturating_add(text.len()),
                            _ => bytes,
                        },
                    );
                    if self.retained_bytes.checked_add(minimum)
                        .is_none_or(|total| total > self.max_bytes) {
                        return Err(Error::Execution("stream set memory budget exceeded".into()));
                    }
                    let key = row.to_vec();
                    let bytes = super::row_footprint::footprint(key.iter());
                    let next = self.retained_bytes.checked_add(bytes)
                        .filter(|total| *total <= self.max_bytes)
                        .ok_or_else(|| Error::Execution("stream set memory budget exceeded".into()))?;
                    self.retained_bytes = next;
                    self.emitted.insert(key);
                    true
                }
            }
        };
        Ok(selected)
    }
}

impl Iterator for SetStream {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        if let Some(error) = self.pending_error.take() {
            self.stopped = true;
            return Some(Err(error));
        }
        // An empty INTERSECT build side cannot match any left row. Removing
        // exhausted keys in both INTERSECT modes lets us stop as soon as
        // every possible result has been emitted.
        // Avoid scanning expensive remaining left partitions in either case.
        if matches!(self.mode, SetMode::IntersectDistinct | SetMode::IntersectAll)
            && self.right_counts.is_empty()
        {
            self.stopped = true;
            return match self.control.check() {
                Ok(()) => None,
                Err(error) => Some(Err(error)),
            };
        }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let Some(next) = self.left.next() else {
                self.stopped = true;
                return None;
            };
            let result: Result<Batch> = (|| {
                let batch = next?;
                if batch.schema() != &self.left_schema {
                    return Err(Error::Schema("stream set left schema changed".into()));
                }
                // When every right-side bag count is exhausted, EXCEPT ALL
                // becomes an identity operator. Move all left columns through
                // without per-row hashing, materialization, or TEXT cloning.
                // Only metadata widening is allowed: INTEGER->FLOAT still
                // needs the normal per-cell conversion below.
                if self.mode == SetMode::ExceptAll && self.right_counts.is_empty()
                    && self.left_schema.fields().iter().zip(self.output_schema.fields())
                        .all(|(source, target)| source.data_type == target.data_type)
                {
                    return batch.relabel(self.output_schema.clone());
                }
                let mut output: Vec<Vec<Value>> = (0..self.output_schema.len())
                    .map(|_| Vec::new()).collect();
                let mut selected = 0usize;
                let mut retained_rows = Vec::new();
                let borrowed = (batch.schema().len() == 1 &&
                    batch.schema().fields()[0].data_type == self.output_schema.fields()[0].data_type)
                    .then(|| batch.column(0)).transpose()?;
                for index in 0..batch.num_rows() {
                    let row_result: Result<Option<Vec<Value>>> = (|| {
                        if index % 1024 == 0 { self.control.check()?; }
                        if let Some(column) = borrowed {
                            let value = &column[index];
                            // Rejected single-column rows require no owned
                            // vector or cloned TEXT cell.
                            if self.select_row(std::slice::from_ref(value))? {
                                retained_rows.push(index);
                            }
                            return Ok(None);
                        }
                        let row = convert_row(&batch, index, &self.output_schema)?;
                        Ok(self.select_row(&row)?.then_some(row))
                    })();
                    match row_result {
                        Ok(Some(row)) => {
                            for (column, value) in output.iter_mut().zip(row) {
                                column.push(value);
                            }
                            selected += 1;
                        }
                        Ok(None) => {}
                        Err(error) if selected > 0 || !retained_rows.is_empty() => {
                            // Emit previously selected rows before the terminal
                            // failure. No later left-side input is consumed.
                            self.pending_error = Some(error);
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if borrowed.is_some() {
                    // The one-column fast path has no type conversion.
                    // Transfer accepted owned values without copying TEXT payloads.
                    return batch.take_sorted_owned(&retained_rows)?
                        .relabel(self.output_schema.clone());
                }
                // Rows come from typed input batches. The only conversion is
                // INTEGER to FLOAT, and the output nullability is widened.
                Batch::from_validated_columns(self.output_schema.clone(), output, selected)
            })();
            match result {
                Ok(batch) if batch.num_rows() == 0 => {
                    if let Some(error) = self.pending_error.take() {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                    continue;
                },
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => { self.stopped = true; return Some(Err(error)); }
            }
        }
    }
}

#[cfg(test)]
mod borrowed_single_column_set_tests {
    use super::*;

    #[test]
    fn duplicate_wide_text_and_null_bags_preserve_all_set_modes() {
        let schema = Schema::new(vec![Field::new("k", DataType::Text, true)]).unwrap();
        let wide = "w".repeat(8192);
        let make = |values: Vec<Value>| Batch::from_rows(schema.clone(),
            values.into_iter().map(|value| vec![value]).collect()).unwrap();
        let left = make(vec![
            Value::Text(wide.clone()), Value::Null,
            Value::Text("other".into()), Value::Text(wide.clone()),
        ]);
        let right = make(std::iter::repeat_n(Value::Text(wide.clone()), 256)
            .chain([Value::Null]).collect());
        let run = |mode| SetStream::new(
            Box::new([Ok(left.clone())].into_iter()),
            Box::new([Ok(right.clone())].into_iter()),
            schema.clone(), schema.clone(), mode, QueryControl::default(),
            4, 20_000,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(run(SetMode::IntersectAll), vec![
            vec![Value::Text(wide.clone())], vec![Value::Null],
            vec![Value::Text(wide.clone())],
        ]);
        assert_eq!(run(SetMode::IntersectDistinct), vec![
            vec![Value::Text(wide.clone())], vec![Value::Null],
        ]);
        assert_eq!(run(SetMode::ExceptDistinct),
            vec![vec![Value::Text("other".into())]]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(values: &[Value]) -> Batch {
        Batch::from_rows(Schema::new(vec![Field::new("v", DataType::Integer, true)]).unwrap(),
            values.iter().cloned().map(|value| vec![value]).collect()).unwrap()
    }
    fn run(mode: SetMode) -> Vec<Value> {
        let left = batch(&[Value::Integer(1), Value::Integer(1), Value::Integer(2),
            Value::Null, Value::Integer(3), Value::Integer(2)]);
        let right = batch(&[Value::Integer(1), Value::Integer(2),
            Value::Integer(2), Value::Null, Value::Integer(4)]);
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        SetStream::new(Box::new([Ok(left)].into_iter()),
            Box::new([Ok(right)].into_iter()), left_schema, right_schema, mode,
            QueryControl::default(), 100, 100_000).unwrap()
            .collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.column(0).unwrap().to_vec()).collect()
    }

    #[test]
    fn distinct_and_all_preserve_sql_row_multiplicity_and_nulls() {
        assert_eq!(run(SetMode::IntersectDistinct),
            vec![Value::Integer(1), Value::Integer(2), Value::Null]);
        assert_eq!(run(SetMode::IntersectAll),
            vec![Value::Integer(1), Value::Integer(2), Value::Null, Value::Integer(2)]);
        assert_eq!(run(SetMode::ExceptDistinct), vec![Value::Integer(3)]);
        assert_eq!(run(SetMode::ExceptAll), vec![Value::Integer(1), Value::Integer(3)]);
    }

    #[test]
    fn right_key_admission_is_bounded() {
        let left = batch(&[]);
        let right = batch(&[Value::Integer(1), Value::Integer(2)]);
        assert!(SetStream::new(Box::new([Ok(left.clone())].into_iter()),
            Box::new([Ok(right.clone())].into_iter()), left.schema().clone(),
            right.schema().clone(), SetMode::IntersectAll, QueryControl::default(),
            1, 10_000).is_err());
    }

    #[test]
    fn preserves_selected_rows_before_late_left_side_memory_error() {
        let left = batch(&[Value::Integer(1), Value::Integer(2), Value::Integer(3)]);
        let right = batch(&[]);
        let mut stream = SetStream::new(
            Box::new([Ok(left.clone())].into_iter()),
            Box::new([Ok(right.clone())].into_iter()),
            left.schema().clone(), right.schema().clone(),
            SetMode::ExceptDistinct, QueryControl::default(), 2, 4096,
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]);
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }

    #[test]
    fn mismatched_and_numeric_widened_schemas() {
        let integer = Schema::new(vec![Field::new("value", DataType::Integer, true)]).unwrap();
        let float = Schema::new(vec![Field::new("other", DataType::Float, true)]).unwrap();
        let output = set_schema(&integer, &float).unwrap();
        assert_eq!(output.field(0).unwrap().data_type, DataType::Float);
        assert_eq!(set_schema(&integer, &Schema::new(vec![Field::new("v",
            DataType::Text, true)]).unwrap()).is_err(), true);
    }
}

#[cfg(test)]
mod distinct_right_membership_tests {
    use super::*;

    #[test]
    fn repeated_right_keys_use_membership_without_changing_sql_set_results() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(),
            std::iter::repeat_n(vec![Value::Integer(1)], 10_000)
                .chain(std::iter::once(vec![Value::Null])).collect()).unwrap();
        let run = |mode| SetStream::new(
            Box::new([Ok(left.clone())].into_iter()),
            Box::new([Ok(right.clone())].into_iter()),
            schema.clone(), schema.clone(), mode,
            QueryControl::default(), 10, 4096,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(run(SetMode::IntersectDistinct),
            vec![vec![Value::Integer(1)], vec![Value::Null]]);
        assert_eq!(run(SetMode::ExceptDistinct), vec![vec![Value::Integer(2)]]);
        assert_eq!(run(SetMode::IntersectAll),
            vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Null]]);
    }
}

#[cfg(test)]
mod columnar_streaming_set_perf_tests {
    use super::*;

    #[test]
    fn all_semantics_preserve_multiplicity_through_many_batches() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let keys = (0..300_i64).map(|n| vec![
            if n % 17 == 0 { Value::Null } else { Value::Integer(n % 41) },
            if n % 5 == 0 { Value::Null } else { Value::Text(format!("v{}", n % 7)) },
        ]).collect::<Vec<_>>();
        let left = Batch::from_rows(schema.clone(), keys.clone()).unwrap();
        let right = Batch::from_rows(schema.clone(), keys.clone()).unwrap();
        let results = SetStream::new(
            Box::new([Ok(left)].into_iter()),
            Box::new([Ok(right)].into_iter()),
            schema.clone(), schema,
            SetMode::IntersectAll, QueryControl::default(),
            512, 1_000_000,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        let actual = results.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, keys);
    }

    #[test]
    fn zero_column_rows_are_counted_without_materialized_row_vectors() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 3).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let results = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::IntersectAll, QueryControl::default(),
            10, 10_000,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.iter().map(Batch::num_rows).sum::<usize>(), 2);
        assert!(results.iter().all(|batch| batch.schema().is_empty()));
    }
}

#[cfg(test)]
mod intersect_distinct_right_key_reuse_regressions {
    use super::*;

    #[test]
    fn matching_right_keys_need_no_second_memory_admission() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2)], vec![Value::Integer(2)],
            vec![Value::Integer(1)], vec![Value::Integer(3)],
            vec![Value::Integer(1)],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
        ]).unwrap();
        // Both right-hand keys exactly fill the admission budget.
        // INTERSECT DISTINCT must not require another key for its output.
        let result = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 2, 1024,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = result.into_iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(1)]]);
    }

    #[test]
    fn matches_can_cross_batches_without_being_emitted_twice() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, true)]).unwrap();
        let mk = |values: Vec<Value>| Batch::from_rows(schema.clone(),
            values.into_iter().map(|v| vec![v]).collect()).unwrap();
        let result = SetStream::new(
            Box::new([
                Ok(mk(vec![Value::Null, Value::Integer(1)])),
                Ok(mk(vec![Value::Integer(1), Value::Null, Value::Integer(2)])),
            ].into_iter()),
            Box::new([Ok(mk(vec![Value::Integer(1), Value::Null]))].into_iter()),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 2, 1024,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result.into_iter().flat_map(|b| b.rows().unwrap())
            .collect::<Vec<_>>(), vec![vec![Value::Null], vec![Value::Integer(1)]]);
    }
}

#[cfg(test)]
mod trusted_set_output_regressions {
    use super::*;
    #[test]
    fn numeric_coercion_and_null_bag_counts_keep_output_types() {
        let left_schema = Schema::new(vec![
            Field::new("k", DataType::Integer, true)
        ]).unwrap();
        let right_schema = Schema::new(vec![
            Field::new("k", DataType::Float, true)
        ]).unwrap();
        let left = Batch::from_rows(left_schema.clone(), vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(right_schema.clone(), vec![
            vec![Value::Float(2.0)], vec![Value::Null],
        ]).unwrap();
        let output = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            left_schema, right_schema, SetMode::IntersectAll,
            QueryControl::default(), 100, 100_000
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().flat_map(|b| b.rows().unwrap())
            .collect::<Vec<_>>(), vec![
                vec![Value::Float(2.0)], vec![Value::Null],
            ]);
        assert_eq!(output[0].schema().fields()[0].data_type, DataType::Float);
    }
}

#[cfg(test)]
mod intersect_distinct_retained_allocation_tests {
    use super::*;

    #[test]
    fn releasing_right_key_uses_owned_capacity_not_matching_left_capacity() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, false)]).unwrap();
        let mut large_left = String::with_capacity(16_384);
        large_left.push_str("same key");
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("same key".into())],
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(large_left)],
            vec![Value::Text("same key".into())],
        ]).unwrap();
        let mut stream = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 1, 256,
        ).unwrap();
        assert!(stream.retained_bytes > 0);
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Text("same key".into())]]);
        assert_eq!(stream.retained_bytes, 0);
        assert!(stream.next().is_none());
    }

    #[test]
    fn reclaiming_mixed_nullable_keys_tracks_remaining_right_bytes_exactly() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("other", DataType::Integer, false),
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("x".into()), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
        ]).unwrap();
        let mut wide = String::with_capacity(4096);
        wide.push('x');
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(wide), Value::Integer(1)],
        ]).unwrap();
        let mut stream = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 2, 1024,
        ).unwrap();
        let remaining = super::super::row_footprint::footprint(
            [Value::Null, Value::Integer(2)].iter(),
        );
        assert_eq!(stream.next().unwrap().unwrap().num_rows(), 1);
        assert_eq!(stream.retained_bytes, remaining);
    }
}

#[cfg(test)]
mod exhausted_intersect_short_circuit_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn empty_right_input_never_polls_left_for_either_intersection_mode() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        for mode in [SetMode::IntersectDistinct, SetMode::IntersectAll] {
            let left: BatchStream = Box::new([
                Err(Error::Execution("unreachable left source".into())),
            ].into_iter());
            let right: BatchStream = Box::new(std::iter::empty());
            let mut stream = SetStream::new(
                left, right, schema.clone(), schema.clone(), mode,
                QueryControl::default(), 100, 4096,
            ).unwrap();
            assert!(stream.next().is_none());
            assert!(stream.next().is_none());
        }
    }

    #[test]
    fn intersect_distinct_stops_when_last_build_key_is_emitted() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let value = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(17)],
        ]).unwrap();
        let left: BatchStream = Box::new([
            Ok(value.clone()),
            Err(Error::Execution("unreachable after all keys matched".into())),
        ].into_iter());
        let right: BatchStream = Box::new([Ok(value)].into_iter());
        let mut stream = SetStream::new(
            left, right, schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 100, 4096,
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(17)]);
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod except_distinct_compact_key_admission_tests {
    use super::*;

    #[test]
    fn overallocated_left_text_does_not_exhaust_hash_set_key_budget() {
        let schema = Schema::new(vec![Field::new("k", DataType::Text, false)]).unwrap();
        let mut text = String::with_capacity(16_384);
        text.push_str("small");
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(text)],
            vec![Value::Text("small".into())],
            vec![Value::Text("second".into())],
        ]).unwrap();
        let stream = SetStream::new(
            Box::new([Ok(left)].into_iter()),
            Box::new(std::iter::empty()), schema.clone(), schema,
            SetMode::ExceptDistinct, QueryControl::default(), 2, 256,
        ).unwrap();
        let rows = stream.collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Text("small".into())],
            vec![Value::Text("second".into())],
        ]);
    }

    #[test]
    fn oversized_real_key_still_errors_after_valid_prefix() {
        let schema = Schema::new(vec![Field::new("k", DataType::Text, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("good".into())],
            vec![Value::Text("x".repeat(2048))],
        ]).unwrap();
        let mut stream = SetStream::new(
            Box::new([Ok(left)].into_iter()),
            Box::new(std::iter::empty()), schema.clone(), schema,
            SetMode::ExceptDistinct, QueryControl::default(), 2, 256,
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Text("good".into())]]);
        assert!(matches!(stream.next(), Some(Err(Error::Execution(_)))));
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod owned_set_output_tests {
    use super::*;

    #[test]
    fn selected_text_keeps_the_left_input_allocation() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, false)]).unwrap();
        let payload = "spill-safe-".repeat(20_000);
        let ptr = payload.as_ptr();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(payload)],
            vec![Value::Text("absent".into())],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("spill-safe-".repeat(20_000))],
        ]).unwrap();
        let mut stream = SetStream::new(
            Box::new(std::iter::once(Ok(left))),
            Box::new(std::iter::once(Ok(right))),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 8, 2_000_000,
        ).unwrap();
        let batch = stream.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(matches!(batch.value(0, 0).unwrap(), Value::Text(text) if text.as_ptr() == ptr));
        assert!(stream.next().is_none());
    }

    #[test]
    fn limit_failure_preserves_selected_prefix_and_stops() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(4)],
        ]).unwrap();
        let right = Batch::empty(schema.clone());
        let mut stream = SetStream::new(
            Box::new(std::iter::once(Ok(left))),
            Box::new(std::iter::once(Ok(right))),
            schema.clone(), schema, SetMode::ExceptDistinct,
            QueryControl::default(), 1, 1024,
        ).unwrap();
        assert_eq!(stream.next().unwrap().unwrap().rows().unwrap(), vec![vec![Value::Integer(3)]]);
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod exhausted_bag_key_eviction_tests {
    use super::*;

    #[test]
    fn intersect_all_releases_consumed_keys_and_does_not_poll_later_batches() {
        let schema = Schema::new(vec![Field::new("k", DataType::Text, true)]).unwrap();
        let batch = |values: Vec<Value>| Batch::from_rows(
            schema.clone(), values.into_iter().map(|value| vec![value]).collect(),
        ).unwrap();
        let right = batch(vec![Value::Text("wide".repeat(200)), Value::Null]);
        let first = batch(vec![Value::Null, Value::Text("wide".repeat(200))]);
        let left: BatchStream = Box::new([
            Ok(first),
            Err(Error::Execution("must not poll exhausted intersection".into())),
        ].into_iter());
        let mut stream = SetStream::new(
            left, Box::new([Ok(right)].into_iter()), schema.clone(), schema,
            SetMode::IntersectAll, QueryControl::default(), 4, 4096,
        ).unwrap();
        assert_eq!(stream.right_counts.len(), 2);
        let rows = stream.next().unwrap().unwrap().rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::Null]);
        assert_eq!(rows[1], vec![Value::Text("wide".repeat(200))]);
        assert!(stream.right_counts.is_empty());
        assert_eq!(stream.retained_bytes, 0);
        assert!(stream.next().is_none());
    }

    #[test]
    fn except_all_removes_exhausted_keys_without_losing_left_multiplicity() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let batch = |values: Vec<Value>| Batch::from_rows(
            schema.clone(), values.into_iter().map(|v| vec![v]).collect(),
        ).unwrap();
        let right = batch(vec![Value::Integer(1), Value::Integer(1), Value::Null]);
        let left = batch(vec![
            Value::Integer(1), Value::Null, Value::Integer(1),
            Value::Integer(1), Value::Null, Value::Integer(2),
        ]);
        let mut stream = SetStream::new(
            Box::new([Ok(left)].into_iter()), Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::ExceptAll, QueryControl::default(),
            4, 4096,
        ).unwrap();
        let values = stream.next().unwrap().unwrap().column(0).unwrap().to_vec();
        assert_eq!(values, vec![Value::Integer(1), Value::Null, Value::Integer(2)]);
        assert!(stream.right_counts.is_empty());
        assert_eq!(stream.retained_bytes, 0);
        assert!(stream.next().is_none());
    }
}

#[cfg(test)]
mod exhausted_except_all_passthrough_tests {
    use super::*;

    #[test]
    fn empty_build_moves_wide_multi_column_text_without_clone() {
        let left_schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("text", DataType::Text, false),
        ]).unwrap();
        let right_schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let input = Batch::from_rows(left_schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("🙂".repeat(40_000))],
            vec![Value::Integer(2), Value::Text("x".repeat(20_000))],
        ]).unwrap();
        let first_ptr = match &input.column(1).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        };
        let mut stream = SetStream::new(
            Box::new([Ok(input.clone())].into_iter()),
            Box::new(std::iter::empty()),
            left_schema, right_schema, SetMode::ExceptAll,
            QueryControl::default(), 100, 1024,
        ).unwrap();
        // Use the original owned batch to check allocation identity below.
        let _ = stream.next().unwrap().unwrap();
        let left_schema = input.schema().clone();
        let right_schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let mut stream = SetStream::new(
            Box::new([Ok(input)].into_iter()), Box::new(std::iter::empty()),
            left_schema, right_schema, SetMode::ExceptAll,
            QueryControl::default(), 100, 1024,
        ).unwrap();
        let actual = stream.next().unwrap().unwrap();
        assert_eq!(actual.num_rows(), 2);
        assert!(actual.schema().fields().iter().all(|field| field.nullable));
        let output_ptr = match &actual.column(1).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        };
        assert_eq!(output_ptr, first_ptr);
        assert!(stream.next().is_none());
    }

    #[test]
    fn exhausted_right_counts_unlock_passthrough_but_numeric_widening_still_converts() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, false),
        ]).unwrap();
        let consumed = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Text("taken".into())],
        ]).unwrap();
        let tail = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2), Value::Text("wide".repeat(8192))],
        ]).unwrap();
        let ptr = match &tail.column(1).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        };
        let mut stream = SetStream::new(
            Box::new([Ok(consumed.clone()), Ok(tail)].into_iter()),
            Box::new([Ok(consumed)].into_iter()),
            schema.clone(), schema, SetMode::ExceptAll,
            QueryControl::default(), 4, 50_000,
        ).unwrap();
        let after = stream.next().unwrap().unwrap();
        assert_eq!(after.column(0).unwrap(), &[Value::Integer(2)]);
        assert_eq!(match &after.column(1).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!(),
        }, ptr);
        assert!(stream.next().is_none());

        let integer = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let float = Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap();
        let batch = Batch::from_rows(integer.clone(), vec![vec![Value::Integer(7)]]).unwrap();
        let converted = SetStream::new(
            Box::new([Ok(batch)].into_iter()), Box::new(std::iter::empty()),
            integer, float, SetMode::ExceptAll, QueryControl::default(), 4, 1024,
        ).unwrap().next().unwrap().unwrap();
        assert_eq!(converted.column(0).unwrap(), &[Value::Float(7.0)]);
    }
}

#[cfg(test)]
mod clustered_right_set_build_tests {
    use super::*;

    #[test]
    fn repeated_unicode_composite_keys_match_distinct_and_all_bag_semantics() {
        let schema = Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("number", DataType::Float, true),
        ]).unwrap();
        let long = "é🌱".repeat(2048);
        let row = |text: Option<&str>, number: Option<f64>| vec![
            text.map_or(Value::Null, |value| Value::Text(value.into())),
            number.map_or(Value::Null, Value::Float),
        ];
        let left = Batch::from_rows(schema.clone(), vec![
            row(Some(&long), Some(0.0)), row(Some(&long), Some(-0.0)),
            row(None, Some(5.0)), row(Some("other"), None),
            row(None, Some(5.0)),
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            row(Some(&long), Some(-0.0)), row(Some(&long), Some(0.0)),
            row(Some(&long), Some(0.0)), row(None, Some(5.0)),
            row(None, Some(5.0)), row(None, Some(5.0)),
        ]).unwrap();
        let run = |mode| SetStream::new(
            Box::new([Ok(left.clone())].into_iter()),
            Box::new([Ok(right.clone())].into_iter()),
            schema.clone(), schema.clone(), mode, QueryControl::default(), 3, usize::MAX,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap()
            .into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(run(SetMode::IntersectDistinct),
            vec![row(Some(&long), Some(0.0)), row(None, Some(5.0))]);
        assert_eq!(run(SetMode::ExceptDistinct),
            vec![row(Some("other"), None)]);
        assert_eq!(run(SetMode::IntersectAll),
            vec![row(Some(&long), Some(0.0)), row(Some(&long), Some(-0.0)),
                 row(None, Some(5.0)), row(None, Some(5.0))]);
        assert_eq!(run(SetMode::ExceptAll),
            vec![row(Some("other"), None)]);
    }

    #[test]
    fn repeated_distinct_keys_do_not_exhaust_right_key_budget() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Integer, true),
            Field::new("b", DataType::Text, true),
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(),
            std::iter::repeat_n(vec![Value::Null, Value::Text("same".into())], 2000)
                .collect()).unwrap();
        let left = Batch::from_rows(schema.clone(),
            vec![vec![Value::Null, Value::Text("same".into())]]).unwrap();
        let result = SetStream::new(
            Box::new([Ok(left)].into_iter()),
            Box::new([Ok(right)].into_iter()),
            schema.clone(), schema, SetMode::IntersectDistinct,
            QueryControl::default(), 1, 4096,
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result.iter().map(Batch::num_rows).sum::<usize>(), 1);
    }
}
