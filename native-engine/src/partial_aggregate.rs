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
    // Accumulate a sorted key run transactionally, with one checked row
    // count and one state write rather than repeating both for each cell.
    fn add_run(&mut self, amounts: &[Value]) -> Result<()> {
        let count = i64::try_from(amounts.len())
            .map_err(|_| Error::Execution("group count overflow".into()))?;
        let rows = self.rows.checked_add(count)
            .ok_or_else(|| Error::Execution("group count overflow".into()))?;
        let mut sum = self.sum;
        let mut has_value = self.has_value;
        for value in amounts {
            match value {
                Value::Integer(number) => {
                    sum = sum.checked_add(i128::from(*number))
                        .ok_or_else(|| Error::Execution("partial SUM overflow".into()))?;
                    has_value = true;
                }
                Value::Null => {}
                _ => return Err(Error::Type("partial SUM requires integer values".into())),
            }
        }
        self.rows = rows;
        self.sum = sum;
        self.has_value = has_value;
        Ok(())
    }
}

fn key_footprint(key: &Value) -> Result<usize> {
    // Account for the owned hash-map entry and its optional string allocation.
    let fixed = std::mem::size_of::<Value>() + std::mem::size_of::<State>() + 64;
    let text_bytes = match key { Value::Text(text) => text.len(), _ => 0 };
    fixed.checked_add(text_bytes)
        .ok_or_else(|| Error::Execution("partial aggregation key size overflow".into()))
}

#[derive(Clone, Debug)]
pub struct PartialGroupSum {
    key_column: String,
    amount_column: String,
    key_type: Option<DataType>,
    max_groups: usize,
    max_key_bytes: usize,
    retained_key_bytes: usize,
    groups: HashMap<Value, State>,
    failed: bool,
}

impl PartialGroupSum {
    pub fn new(key: impl Into<String>, amount: impl Into<String>, max_groups: usize) -> Self {
        Self::with_key_byte_limit(key, amount, max_groups, 64 * 1024 * 1024)
    }

    /// Bound retained group keys as well as their cardinality.
    pub fn with_key_byte_limit(
        key: impl Into<String>, amount: impl Into<String>,
        max_groups: usize, max_key_bytes: usize,
    ) -> Self {
        Self {
            key_column: key.into(), amount_column: amount.into(),
            key_type: None, max_groups, max_key_bytes, retained_key_bytes: 0,
            groups: HashMap::new(), failed: false,
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
        // A sorted or locally partitioned input can contain thousands of
        // adjacent copies of the same key. Hash once per run rather than
        // acquiring a HashMap entry for every input row. Still accumulate
        // each value with the checked State::add path.
        let mut start = 0;
        while start < key_values.len() {
            let key_value = &key_values[start];
            let mut end = start + 1;
            while end < key_values.len() && key_values[end] == *key_value {
                end += 1;
            }
            if let Some(state) = self.groups.get_mut(key_value) {
                state.add_run(&amounts[start..end])?;
            } else {
                if self.groups.len() >= self.max_groups {
                    return Err(Error::Execution("group cardinality exceeds memory admission limit".into()));
                }
                let next_bytes = self.retained_key_bytes.checked_add(key_footprint(key_value)?)
                    .filter(|&size| size <= self.max_key_bytes)
                    .ok_or_else(|| Error::Execution("partial aggregation key memory budget exceeded".into()))?;
                let mut state = State::default();
                state.add_run(&amounts[start..end])?;
                self.groups.insert(key_value.clone(), state);
                self.retained_key_bytes = next_bytes;
            }
            start = end;
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
        let mut merged_key_bytes = self.retained_key_bytes;
        for (key, state) in &other.groups {
            // Reuse the lookup for admission and overflow preflight instead of
            // hashing every incoming group twice.
            let current = self.groups.get(key);
            if current.is_none() {
                new_groups = new_groups.checked_add(1)
                    .ok_or_else(|| Error::Execution("group cardinality overflow".into()))?;
                merged_key_bytes = merged_key_bytes.checked_add(key_footprint(key)?)
                    .filter(|&size| size <= self.max_key_bytes)
                    .ok_or_else(|| Error::Execution("merged group key memory budget exceeded".into()))?;
            }
            if self.groups.len().checked_add(new_groups)
                .is_none_or(|count| count > self.max_groups) {
                return Err(Error::Execution("merged group cardinality exceeds limit".into()));
            }
            let empty = State::default();
            let current = current.unwrap_or(&empty);
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
        self.retained_key_bytes = merged_key_bytes;
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
        let rows = self.groups.len();
        // Retained keys are already typed native values. Move their strings
        // into final output columns instead of allocating a row vector for
        // every group and transposing it during Batch::from_rows.
        let mut keys = Vec::with_capacity(rows);
        let mut counts = Vec::with_capacity(rows);
        let mut sums = Vec::with_capacity(rows);
        for (key, state) in self.groups {
            let sum = if state.has_value {
                Value::Integer(i64::try_from(state.sum)
                    .map_err(|_| Error::Execution("group sum exceeds i64".into()))?)
            } else {
                Value::Null
            };
            keys.push(key);
            counts.push(Value::Integer(state.rows));
            sums.push(sum);
        }
        Batch::from_validated_columns(schema, vec![keys, counts, sums], rows)
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

#[cfg(test)]
mod partial_aggregate_single_lookup_merge_tests {
    use super::*;

    #[test]
    fn preflights_new_and_existing_groups_before_merge() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, false),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("a".into()), Value::Integer(7)],
            vec![Value::Text("b".into()), Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Text("a".into()), Value::Integer(5)],
            vec![Value::Text("c".into()), Value::Integer(-2)],
        ]).unwrap();
        let mut first = PartialGroupSum::new("key", "amount", 3);
        let mut second = PartialGroupSum::new("key", "amount", 3);
        first.update(&left).unwrap();
        second.update(&right).unwrap();
        first.merge(second).unwrap();
        let result = first.finish().unwrap();
        let mut rows = result.rows().unwrap();
        rows.sort_by(|a, b| a[0].sql_compare(&b[0]).unwrap().unwrap());
        assert_eq!(rows, vec![
            vec![Value::Text("a".into()), Value::Integer(2), Value::Integer(12)],
            vec![Value::Text("b".into()), Value::Integer(1), Value::Null],
            vec![Value::Text("c".into()), Value::Integer(1), Value::Integer(-2)],
        ]);
    }
}

#[cfg(test)]
mod partial_group_key_byte_budget_regressions {
    use super::*;

    #[test]
    fn repeated_keys_do_not_consume_extra_admission_budget() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Text, false),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap();
        let one = Batch::from_rows(schema.clone(), (0..50)
            .map(|_| vec![Value::Text("a".repeat(40)), Value::Integer(1)])
            .collect()).unwrap();
        let mut agg = PartialGroupSum::with_key_byte_limit("k", "amount", 10, 200);
        agg.update(&one).unwrap();
        assert_eq!(agg.groups.len(), 1);
        assert_eq!(agg.retained_key_bytes, key_footprint(&Value::Text("a".repeat(40))).unwrap());
        assert_eq!(agg.finish().unwrap().row(0).unwrap()[2], Value::Integer(50));

        let mut agg = PartialGroupSum::with_key_byte_limit("k", "amount", 10, 200);
        agg.update(&one).unwrap();
        let other = Batch::from_rows(schema, vec![
            vec![Value::Text("b".repeat(40)), Value::Integer(1)],
        ]).unwrap();
        assert!(agg.update(&other).is_err());
        assert_eq!(agg.groups.len(), 1);
        assert!(agg.finish().is_err());
    }

    #[test]
    fn merge_preflights_retained_key_bytes_before_writing_groups() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Text, false),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap();
        let single = |key: &str| Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(key.repeat(40)), Value::Integer(1)],
        ]).unwrap();
        let mut left = PartialGroupSum::with_key_byte_limit("k", "amount", 10, 200);
        let mut right = PartialGroupSum::with_key_byte_limit("k", "amount", 10, 200);
        left.update(&single("a")).unwrap();
        right.update(&single("b")).unwrap();
        let original = left.groups.clone();
        assert!(left.merge(right).is_err());
        assert_eq!(left.groups.len(), original.len());
        assert!(left.failed);
    }
}

#[cfg(test)]
mod columnar_partial_finalization_tests {
    use super::*;

    #[test]
    fn wide_group_keys_keep_counts_nulls_and_sum_values() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Text, true),
            Field::new("v", DataType::Integer, true),
        ]).unwrap();
        let wide = "🙂é".repeat(4096);
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text(wide.clone()), Value::Integer(7)],
            vec![Value::Text(wide.clone()), Value::Integer(5)],
            vec![Value::Null, Value::Null],
            vec![Value::Text("other".into()), Value::Null],
        ]).unwrap();
        let mut partial = PartialGroupSum::new("k", "v", 16);
        partial.update(&batch).unwrap();
        let result = partial.finish().unwrap();
        let rows = result.rows().unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![
            Value::Text(wide), Value::Integer(2), Value::Integer(12),
        ]));
        assert!(rows.contains(&vec![Value::Null, Value::Integer(1), Value::Null]));
        assert!(rows.contains(&vec![
            Value::Text("other".into()), Value::Integer(1), Value::Null,
        ]));
    }

    #[test]
    fn empty_group_map_keeps_typed_output_schema() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Integer, false),
            Field::new("v", DataType::Integer, true),
        ]).unwrap();
        let mut partial = PartialGroupSum::new("k", "v", 16);
        partial.update(&Batch::empty(schema)).unwrap();
        let result = partial.finish().unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema().len(), 3);
    }
}

#[cfg(test)]
mod partial_aggregate_run_tests {
    use super::*;

    fn input(rows: Vec<(Value, Value)>) -> Batch {
        Batch::from_rows(
            Schema::new(vec![
                Field::new("key", DataType::Text, true),
                Field::new("amount", DataType::Integer, true),
            ]).unwrap(),
            rows.into_iter().map(|(key, value)| vec![key, value]).collect(),
        ).unwrap()
    }

    #[test]
    fn repeated_keys_hash_once_per_run_without_changing_totals() {
        let a = Value::Text("🦀".repeat(1000));
        let b = Value::Text("b".repeat(1000));
        let mut rows = Vec::new();
        for _ in 0..512 { rows.push((a.clone(), Value::Integer(2))); }
        for _ in 0..128 { rows.push((b.clone(), Value::Null)); }
        for _ in 0..512 { rows.push((a.clone(), Value::Integer(3))); }
        rows.push((Value::Null, Value::Integer(4)));
        let batch = input(rows);
        let mut aggregate = PartialGroupSum::with_key_byte_limit(
            "key", "amount", 3, 10_000
        );
        aggregate.update(&batch).unwrap();
        let output = aggregate.finish().unwrap();
        let keys = output.column(0).unwrap();
        let counts = output.column(1).unwrap();
        let sums = output.column(2).unwrap();
        let mut actual = std::collections::HashMap::new();
        for i in 0..output.num_rows() {
            actual.insert(keys[i].clone(), (counts[i].clone(), sums[i].clone()));
        }
        assert_eq!(actual[&a], (Value::Integer(1024), Value::Integer(2560)));
        assert_eq!(actual[&b], (Value::Integer(128), Value::Null));
        assert_eq!(actual[&Value::Null], (Value::Integer(1), Value::Integer(4)));
    }

    #[test]
    fn run_admission_rejects_new_keys_and_invalidates_the_aggregate() {
        let mut aggregate = PartialGroupSum::new("key", "amount", 1);
        let batch = input(vec![
            (Value::Text("x".into()), Value::Integer(1)),
            (Value::Text("x".into()), Value::Integer(2)),
            (Value::Text("y".into()), Value::Integer(3)),
        ]);
        assert!(aggregate.update(&batch).is_err());
        assert!(aggregate.update(&batch).is_err());
        assert!(aggregate.finish().is_err());
    }
}

#[cfg(test)]
mod partial_run_accumulation_regressions {
    use super::*;

    #[test]
    fn large_adjacent_runs_preserve_nullability_negative_sums_and_counts() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap();
        let rows = (0..4096).map(|n| vec![
            Value::Integer(7),
            if n % 3 == 0 { Value::Null } else if n % 2 == 0 {
                Value::Integer(-3)
            } else { Value::Integer(5) },
        ]).collect::<Vec<_>>();
        let expected = rows.iter().map(|row| match &row[1] {
            Value::Integer(value) => *value, _ => 0
        }).sum::<i64>();
        let batch = Batch::from_rows(schema, rows).unwrap();
        let mut aggregate = PartialGroupSum::new("key", "amount", 4);
        aggregate.update(&batch).unwrap();
        assert_eq!(aggregate.finish().unwrap().row(0).unwrap(), vec![
            Value::Integer(7), Value::Integer(4096), Value::Integer(expected),
        ]);
    }

    #[test]
    fn invalid_runs_and_row_overflow_leave_previous_state_unchanged() {
        let original = State { rows: 9, sum: 20, has_value: true };
        let mut state = original.clone();
        assert!(state.add_run(&[Value::Integer(7), Value::Text("bad".into())]).is_err());
        assert_eq!((state.rows, state.sum, state.has_value),
            (original.rows, original.sum, original.has_value));
        let mut state = State { rows: i64::MAX, ..original };
        assert!(state.add_run(&[Value::Integer(1)]).is_err());
        assert_eq!(state.rows, i64::MAX);
    }
}
