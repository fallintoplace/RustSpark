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

impl State {
    fn add(&mut self, amount: &Value) -> Result<()> {
        let rows = self.rows.checked_add(1)
            .ok_or_else(|| Error::Execution("group count overflow".into()))?;
        let (sum, has_value) = match amount {
            Value::Integer(value) => (
                self.sum.checked_add(i128::from(*value))
                    .ok_or_else(|| Error::Execution("partial SUM overflow".into()))?,
                true,
            ),
            Value::Null => (self.sum, self.has_value),
            _ => return Err(Error::Type("partial SUM requires integer values".into())),
        };
        self.rows = rows;
        self.sum = sum;
        self.has_value = has_value;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PartialGroupSum {
    key_column: String,
    amount_column: String,
    key_type: Option<DataType>,
    max_groups: usize,
    groups: HashMap<Value, State>,
    failed: bool,
}

impl PartialGroupSum {
    pub fn new(key: impl Into<String>, amount: impl Into<String>, max_groups: usize) -> Self {
        Self {
            key_column: key.into(), amount_column: amount.into(),
            key_type: None, max_groups, groups: HashMap::new(), failed: false,
        }
    }

    pub fn update(&mut self, batch: &Batch) -> Result<()> {
        if self.failed {
            return Err(Error::Execution("partial aggregation is invalid after a previous error".into()));
        }
        let result = self.update_inner(batch);
        if result.is_err() { self.failed = true; }
        result
    }

    fn update_inner(&mut self, batch: &Batch) -> Result<()> {
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
        let key_values = batch.column(key)?;
        let amounts = batch.column(amount)?;
        for (key_value, value) in key_values.iter().zip(amounts) {
            if let Some(state) = self.groups.get_mut(key_value) {
                state.add(value)?;
            } else {
                if self.groups.len() >= self.max_groups {
                    return Err(Error::Execution("group cardinality exceeds memory admission limit".into()));
                }
                let mut state = State::default();
                state.add(value)?;
                self.groups.insert(key_value.clone(), state);
            }
        }
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        if self.failed || other.failed {
            return Err(Error::Execution("cannot merge a failed partial aggregation".into()));
        }
        let result = self.merge_inner(other);
        if result.is_err() { self.failed = true; }
        result
    }

    fn merge_inner(&mut self, other: Self) -> Result<()> {
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
        if self.failed {
            return Err(Error::Execution("partial aggregation is invalid after a previous error".into()));
        }
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
mod failed_partial_aggregation_tests {
    use super::*;

    #[test]
    fn cannot_retry_or_finish_after_partial_cardinality_failure() {
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(4)],
        ]).unwrap();
        let mut state = PartialGroupSum::new("g", "n", 1);
        assert!(state.update(&input).is_err());
        let valid = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Integer(5)],
        ]).unwrap();
        assert!(state.update(&valid).is_err());
        assert!(state.finish().is_err());
    }
}

#[cfg(test)]
mod consolidated_partial_merge_preflight_tests {
    use super::*;
    #[test]
    fn overflow_keeps_all_previously_aggregated_groups_unmodified() {
        let mut first = PartialGroupSum::new("key", "amount", 3);
        first.key_type = Some(DataType::Integer);
        first.groups.insert(Value::Integer(1), State { rows: 1, sum: 5, has_value: true });
        first.groups.insert(Value::Integer(2), State { rows: i64::MAX, sum: 10, has_value: true });
        let old = first.groups.clone();
        let mut second = PartialGroupSum::new("key", "amount", 3);
        second.key_type = Some(DataType::Integer);
        second.groups.insert(Value::Integer(1), State { rows: 1, sum: 10, has_value: true });
        second.groups.insert(Value::Integer(2), State { rows: 1, sum: 20, has_value: true });
        assert!(first.merge(second).is_err());
        for (key, previous) in old {
            let now = &first.groups[&key];
            assert_eq!((now.rows, now.sum, now.has_value), (previous.rows, previous.sum, previous.has_value));
        }
    }
}
