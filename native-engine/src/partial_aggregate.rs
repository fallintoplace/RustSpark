use std::collections::HashMap;

use crate::{
    batch::Batch,
    scalar::Value,
    schema::{DataType, Field, Schema},
    Error, Result,
};

#[derive(Clone, Debug, Default)]
struct State {
    rows: i64,
    sum: i128,
    has_value: bool,
}

#[derive(Clone, Debug)]
pub struct PartialGroupSum {
    key_column: String,
    amount_column: String,
    key_type: Option<DataType>,
    max_groups: usize,
    groups: HashMap<Value, State>,
}

impl PartialGroupSum {
    pub fn new(key: impl Into<String>, amount: impl Into<String>, max_groups: usize) -> Self {
        Self {
            key_column: key.into(), amount_column: amount.into(),
            key_type: None, max_groups, groups: HashMap::new(),
        }
    }

    pub fn update(&mut self, batch: &Batch) -> Result<()> {
        let key = batch.schema().column(&self.key_column)?;
        let amount = batch.schema().column(&self.amount_column)?;
        if batch.schema().field(amount)?.data_type != DataType::Integer {
            return Err(Error::Type("native partial SUM requires integer values".into()));
        }
        let kind = batch.schema().field(key)?.data_type;
        if self.key_type.is_some_and(|previous| previous != kind) {
            return Err(Error::Type("group key schema changed between partitions".into()));
        }
        self.key_type = Some(kind);
        for row in 0..batch.num_rows() {
            let key_value = batch.value(row, key)?.clone();
            if !self.groups.contains_key(&key_value) && self.groups.len() >= self.max_groups {
                return Err(Error::Execution("group cardinality exceeds memory admission limit".into()));
            }
            let state = self.groups.entry(key_value).or_default();
            state.rows = state.rows.checked_add(1)
                .ok_or_else(|| Error::Execution("group count overflow".into()))?;
            match batch.value(row, amount)? {
                Value::Integer(value) => {
                    state.sum = state.sum.checked_add(i128::from(*value))
                        .ok_or_else(|| Error::Execution("partial SUM overflow".into()))?;
                    state.has_value = true;
                }
                Value::Null => {}
                _ => return Err(Error::Type("partial SUM requires integer values".into())),
            }
        }
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        if self.key_column != other.key_column || self.amount_column != other.amount_column {
            return Err(Error::Schema("cannot merge different grouped aggregations".into()));
        }
        if let (Some(a), Some(b)) = (self.key_type, other.key_type) {
            if a != b {
                return Err(Error::Schema("cannot merge different key types".into()));
            }
        }
        let mut new_groups = 0usize;
        for (key, state) in &other.groups {
            if !self.groups.contains_key(key) {
                new_groups = new_groups.checked_add(1)
                    .ok_or_else(|| Error::Execution("group cardinality overflow".into()))?;
            }
            if self.groups.len().checked_add(new_groups)
                .is_none_or(|count| count > self.max_groups) {
                return Err(Error::Execution("merged group cardinality exceeds limit".into()));
            }
            let empty = State::default();
            let current = self.groups.get(key).unwrap_or(&empty);
            current.rows.checked_add(state.rows)
                .ok_or_else(|| Error::Execution("group count overflow".into()))?;
            current.sum.checked_add(state.sum)
                .ok_or_else(|| Error::Execution("group sum overflow".into()))?;
        }
        for (key, state) in other.groups {
            let entry = self.groups.entry(key).or_default();
            entry.rows += state.rows;
            entry.sum += state.sum;
            entry.has_value |= state.has_value;
        }
        self.key_type = self.key_type.or(other.key_type);
        Ok(())
    }

    pub fn finish(self) -> Result<Batch> {
        let kind = self.key_type.ok_or_else(|| Error::Schema("group key type is unknown".into()))?;
        let schema = Schema::new(vec![
            Field::new(self.key_column, kind, true),
            Field::new("row_count", DataType::Integer, false),
            Field::new("sum", DataType::Integer, true),
        ])?;
        let mut rows = Vec::with_capacity(self.groups.len());
        for (key, state) in self.groups {
            let sum = if state.has_value {
                Value::Integer(i64::try_from(state.sum)
                    .map_err(|_| Error::Execution("group sum exceeds i64".into()))?)
            } else {
                Value::Null
            };
            rows.push(vec![key, Value::Integer(state.rows), sum]);
        }
        Batch::from_rows(schema, rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(rows: Vec<Vec<Value>>) -> Batch {
        Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap(), rows).unwrap()
    }

    #[test]
    fn merged_partials_preserve_null_keys_and_sums() {
        let mut a = PartialGroupSum::new("key", "amount", 3);
        let mut b = PartialGroupSum::new("key", "amount", 3);
        a.update(&input(vec![
            vec![Value::Null, Value::Integer(4)],
            vec![Value::Text("x".into()), Value::Null],
        ])).unwrap();
        b.update(&input(vec![
            vec![Value::Null, Value::Integer(5)],
            vec![Value::Text("x".into()), Value::Null],
        ])).unwrap();
        a.merge(b).unwrap();
        let rows = a.finish().unwrap().rows().unwrap();
        assert!(rows.contains(&vec![Value::Null, Value::Integer(2), Value::Integer(9)]));
        assert!(rows.contains(&vec![Value::Text("x".into()), Value::Integer(2), Value::Null]));
    }

    #[test]
    fn rejects_unbounded_group_cardinality() {
        let mut groups = PartialGroupSum::new("key", "amount", 1);
        assert!(groups.update(&input(vec![
            vec![Value::Text("x".into()), Value::Integer(1)],
            vec![Value::Text("y".into()), Value::Integer(1)],
        ])).is_err());
    }
}

#[cfg(test)]
mod partial_merge_atomicity_tests {
    use super::*;

    #[test]
    fn overflow_keeps_previously_aggregated_groups() {
        let mut a = PartialGroupSum::new("key", "amount", 2);
        a.key_type = Some(DataType::Integer);
        a.groups.insert(Value::Integer(1), State {
            rows: i64::MAX, sum: 2, has_value: true
        });
        let mut b = PartialGroupSum::new("key", "amount", 2);
        b.key_type = Some(DataType::Integer);
        b.groups.insert(Value::Integer(1), State {
            rows: 1, sum: 3, has_value: true
        });
        assert!(a.merge(b).is_err());
        assert_eq!(a.groups[&Value::Integer(1)].rows, i64::MAX);
        assert_eq!(a.groups[&Value::Integer(1)].sum, 2);
    }
}
