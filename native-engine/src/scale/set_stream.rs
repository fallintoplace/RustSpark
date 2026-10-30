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
        let mut retained_bytes = 0usize;
        // Bounded materialization of right-hand distinct keys, not its row count.
        for batch in right {
            control.check()?;
            let batch = batch?;
            if batch.schema() != &right_schema {
                return Err(Error::Schema("stream set right schema changed".into()));
            }
            for row in 0..batch.num_rows() {
                if row % 1024 == 0 { control.check()?; }
                let values = convert_row(&batch, row, &output_schema)?;
                let at_limit = right_counts.len() >= max_keys;
                match right_counts.entry(values) {
                    Entry::Occupied(mut entry) => {
                        let next = entry.get().checked_add(1)
                            .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
                        *entry.get_mut() = next;
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
            mode, control, max_keys, max_bytes, retained_bytes, stopped: false,
        })
    }

    fn select_row(&mut self, row: &Vec<Value>) -> Result<bool> {
        let selected = match self.mode {
            SetMode::IntersectAll => {
                if let Some(remaining) = self.right_counts.get_mut(row) {
                    if *remaining == 0 { false }
                    else { *remaining -= 1; true }
                } else { false }
            }
            SetMode::ExceptAll => {
                if let Some(remaining) = self.right_counts.get_mut(row) {
                    if *remaining == 0 { true }
                    else { *remaining -= 1; false }
                } else { true }
            }
            SetMode::IntersectDistinct | SetMode::ExceptDistinct => {
                let belongs = self.right_counts.contains_key(row);
                let wanted = if self.mode == SetMode::IntersectDistinct {
                    belongs
                } else { !belongs };
                if !wanted || self.emitted.contains(row) { false }
                else {
                    let bytes = footprint(row);
                    let next = self.retained_bytes.checked_add(bytes)
                        .filter(|total| *total <= self.max_bytes);
                    if self.emitted.len() + self.right_counts.len() >= self.max_keys ||
                        next.is_none() {
                        return Err(Error::Execution("stream set memory budget exceeded".into()));
                    }
                    self.retained_bytes = next.expect("checked set budget");
                    self.emitted.insert(row.clone());
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
                let mut output = Vec::new();
                for index in 0..batch.num_rows() {
                    if index % 1024 == 0 { self.control.check()?; }
                    let row = convert_row(&batch, index, &self.output_schema)?;
                    if self.select_row(&row)? { output.push(row); }
                }
                Batch::from_rows(self.output_schema.clone(), output)
            })();
            match result {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => { self.stopped = true; return Some(Err(error)); }
            }
        }
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
    fn mismatched_and_numeric_widened_schemas() {
        let integer = Schema::new(vec![Field::new("value", DataType::Integer, true)]).unwrap();
        let float = Schema::new(vec![Field::new("other", DataType::Float, true)]).unwrap();
        let output = set_schema(&integer, &float).unwrap();
        assert_eq!(output.field(0).unwrap().data_type, DataType::Float);
        assert_eq!(set_schema(&integer, &Schema::new(vec![Field::new("v",
            DataType::Text, true)]).unwrap()).is_err(), true);
    }
}
