use std::cmp::Ordering;

use crate::{batch::Batch, scalar::Value, schema::DataType, Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub rows: u64,
    pub nulls: u64,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self { rows: 0, nulls: 0, min: None, max: None }
    }
}

fn should_replace(
    current: Option<&Value>, candidate: Option<&Value>, direction: Ordering,
) -> Result<bool> {
    match (current, candidate) {
        (Some(current), Some(value)) => Ok(value.sql_compare(current)? == Some(direction)),
        (None, Some(_)) => Ok(true),
        (_, None) => Ok(false),
    }
}

impl ColumnStatistics {
    pub fn from_batch(batch: &Batch, name: &str) -> Result<Self> {
        let column = batch.schema().column(name)?;
        let values = batch.column(column)?;
        if batch.schema().field(column)?.data_type == DataType::Integer {
            // Typed integer statistics need no dynamic comparisons or value clones.
            let rows = u64::try_from(values.len())
                .map_err(|_| Error::Execution("statistics row count overflow".into()))?;
            let mut nulls = 0u64;
            let mut min: Option<i64> = None;
            let mut max: Option<i64> = None;
            for value in values {
                match value {
                    Value::Null => nulls += 1,
                    Value::Integer(n) => {
                        min = Some(min.map_or(*n, |old| old.min(*n)));
                        max = Some(max.map_or(*n, |old| old.max(*n)));
                    }
                    _ => unreachable!("native batch schema was validated"),
                }
            }
            return Ok(Self {
                rows, nulls,
                min: min.map(Value::Integer),
                max: max.map(Value::Integer),
            });
        }
        if batch.schema().field(column)?.data_type == DataType::Float {
            // Native DOUBLE bounds order NaNs after every non-NaN value,
            // and treat signed zero as equal. Keep the first equal extremum.
            let rows = u64::try_from(values.len())
                .map_err(|_| Error::Execution("statistics row count overflow".into()))?;
            let mut nulls = 0u64;
            let mut min: Option<f64> = None;
            let mut max: Option<f64> = None;
            for value in values {
                match value {
                    Value::Null => nulls += 1,
                    Value::Float(n) => {
                        if min.is_none_or(|old|
                            !n.is_nan() && (old.is_nan() || *n < old)) {
                            min = Some(*n);
                        }
                        if max.is_none_or(|old|
                            (n.is_nan() && !old.is_nan()) ||
                            (!n.is_nan() && *n > old)) {
                            max = Some(*n);
                        }
                    }
                    _ => unreachable!("native batch schema was validated"),
                }
            }
            return Ok(Self {
                rows, nulls,
                min: min.map(Value::Float),
                max: max.map(Value::Float),
            });
        }
        if batch.schema().field(column)?.data_type == DataType::Boolean {
            // Boolean bounds need only two flags, not per-row dynamic SQL
            // comparisons or temporary Value clones.
            let rows = u64::try_from(values.len())
                .map_err(|_| Error::Execution("statistics row count overflow".into()))?;
            let mut nulls = 0u64;
            let mut seen_false = false;
            let mut seen_true = false;
            for value in values {
                match value {
                    Value::Null => nulls += 1,
                    Value::Boolean(false) => seen_false = true,
                    Value::Boolean(true) => seen_true = true,
                    _ => unreachable!("native batch schema was validated"),
                }
            }
            return Ok(Self {
                rows, nulls,
                min: if seen_false { Some(Value::Boolean(false)) }
                    else if seen_true { Some(Value::Boolean(true)) } else { None },
                max: if seen_true { Some(Value::Boolean(true)) }
                    else if seen_false { Some(Value::Boolean(false)) } else { None },
            });
        }
        if batch.schema().field(column)?.data_type == DataType::Text {
            // Track borrowed byte-lexicographic extrema and clone the two
            // retained strings once, instead of allocating whenever an
            // incoming value becomes the current min or max.
            let rows = u64::try_from(values.len())
                .map_err(|_| Error::Execution("statistics row count overflow".into()))?;
            let mut nulls = 0u64;
            let mut min: Option<&str> = None;
            let mut max: Option<&str> = None;
            for value in values {
                match value {
                    Value::Null => nulls += 1,
                    Value::Text(text) => {
                        let text = text.as_str();
                        if min.is_none_or(|old| text < old) { min = Some(text); }
                        if max.is_none_or(|old| text > old) { max = Some(text); }
                    }
                    _ => unreachable!("native batch schema was validated"),
                }
            }
            return Ok(Self {
                rows, nulls,
                min: min.map(|value| Value::Text(value.to_owned())),
                max: max.map(|value| Value::Text(value.to_owned())),
            });
        }
        let mut stats = Self::default();
        for value in values { stats.observe(value)?; }
        Ok(stats)
    }

    pub fn observe(&mut self, value: &Value) -> Result<()> {
        let rows = self.rows.checked_add(1)
            .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
        let nulls = if value.is_null() {
            self.nulls.checked_add(1)
                .ok_or_else(|| Error::Execution("statistics NULL count overflow".into()))?
        } else {
            self.nulls
        };
        let candidate = (!value.is_null()).then_some(value);
        let replace_min = should_replace(self.min.as_ref(), candidate, Ordering::Less)?;
        let replace_max = should_replace(self.max.as_ref(), candidate, Ordering::Greater)?;
        self.rows = rows;
        self.nulls = nulls;
        if replace_min {
            self.min = Some(value.clone());
        }
        if replace_max {
            self.max = Some(value.clone());
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        // Combined counters can appear valid even when one input summary
        // reports more NULLs than rows. Reject it before changing state.
        if self.nulls > self.rows || other.nulls > other.rows {
            return Err(Error::Execution("invalid NULL count".into()));
        }
        let rows = self.rows.checked_add(other.rows)
            .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
        let nulls = self.nulls.checked_add(other.nulls)
            .ok_or_else(|| Error::Execution("statistics NULL count overflow".into()))?;
        if nulls > rows {
            return Err(Error::Execution("invalid NULL count".into()));
        }
        // Validate both extrema before committing counters or bounds.
        let replace_min = should_replace(self.min.as_ref(), other.min.as_ref(), Ordering::Less)?;
        let replace_max = should_replace(self.max.as_ref(), other.max.as_ref(), Ordering::Greater)?;
        self.rows = rows;
        self.nulls = nulls;
        if replace_min {
            self.min = other.min.clone();
        }
        if replace_max {
            self.max = other.max.clone();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn statistics_merge_across_batches() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Null], vec![Value::Integer(9)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(-2)], vec![Value::Integer(4)],
        ]).unwrap();
        let mut stats = ColumnStatistics::from_batch(&left, "x").unwrap();
        stats.merge(&ColumnStatistics::from_batch(&right, "x").unwrap()).unwrap();
        assert_eq!((stats.rows, stats.nulls), (4, 1));
        assert_eq!(stats.min, Some(Value::Integer(-2)));
        assert_eq!(stats.max, Some(Value::Integer(9)));
    }
}

 
impl ColumnStatistics {
    /// Build mergeable min/max statistics for a sequence of compatible batches.
    pub fn from_batches(batches: &[Batch], name: &str) -> Result<Self> {
        // Borrow TEXT extrema across all source batches. Computing each batch's
        // temporary statistics copies two potentially very large strings.
        if let Some(first) = batches.first() {
            let index = first.schema().column(name)?;
            if first.schema().field(index)?.data_type == DataType::Integer {
                let mut rows = 0u64;
                let mut nulls = 0u64;
                let mut min: Option<i64> = None;
                let mut max: Option<i64> = None;
                for batch in batches {
                    let column = batch.schema().column(name)?;
                    if batch.schema().field(column)?.data_type != DataType::Integer {
                        return Err(Error::Type("statistics batches disagree on column type".into()));
                    }
                    rows = rows.checked_add(u64::try_from(batch.num_rows())
                        .map_err(|_| Error::Execution("statistics row count overflow".into()))?)
                        .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
                    for value in batch.column(column)? {
                        match value {
                            Value::Null => {
                                nulls = nulls.checked_add(1)
                                    .ok_or_else(|| Error::Execution("statistics NULL count overflow".into()))?;
                            }
                            Value::Integer(number) => {
                                min = Some(min.map_or(*number, |old| old.min(*number)));
                                max = Some(max.map_or(*number, |old| old.max(*number)));
                            }
                            _ => unreachable!("native batch schema was validated"),
                        }
                    }
                }
                return Ok(Self {
                    rows, nulls,
                    min: min.map(Value::Integer),
                    max: max.map(Value::Integer),
                });
            }
            if first.schema().field(index)?.data_type == DataType::Text {
                let mut rows = 0u64;
                let mut nulls = 0u64;
                let mut min: Option<&str> = None;
                let mut max: Option<&str> = None;
                for batch in batches {
                    let column = batch.schema().column(name)?;
                    if batch.schema().field(column)?.data_type != DataType::Text {
                        return Err(Error::Type("statistics batches disagree on column type".into()));
                    }
                    rows = rows.checked_add(u64::try_from(batch.num_rows())
                        .map_err(|_| Error::Execution("statistics row count overflow".into()))?)
                        .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
                    for value in batch.column(column)? {
                        match value {
                            Value::Null => {
                                nulls = nulls.checked_add(1).ok_or_else(||
                                    Error::Execution("statistics NULL count overflow".into()))?;
                            }
                            Value::Text(text) => {
                                let text = text.as_str();
                                if min.is_none_or(|old| text < old) { min = Some(text); }
                                if max.is_none_or(|old| text > old) { max = Some(text); }
                            }
                            _ => unreachable!("native batch schema was validated"),
                        }
                    }
                }
                return Ok(Self {
                    rows,
                    nulls,
                    min: min.map(|text| Value::Text(text.to_owned())),
                    max: max.map(|text| Value::Text(text.to_owned())),
                });
            }
        }
        let mut combined = Self::default();
        let mut expected_type = None;
        for batch in batches {
            let column = batch.schema().column(name)?;
            let field = batch.schema().field(column)?;
            if expected_type.is_some_and(|kind| kind != field.data_type) {
                return Err(Error::Type("statistics batches disagree on column type".into()));
            }
            expected_type = Some(field.data_type);
            // Reuse the batch-local integer fast path and combine its summary.
            combined.merge(&Self::from_batch(batch, name)?)?;
        }
        Ok(combined)
    }
}

#[cfg(test)]
mod multi_batch_statistics_feature_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn merges_stats_and_rejects_mixed_column_types() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(10)], vec![Value::Null]
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Integer(-2)]
        ]).unwrap();
        let merged = ColumnStatistics::from_batches(&[a, b], "n").unwrap();
        assert_eq!((merged.rows, merged.nulls), (3, 1));
        assert_eq!(merged.min, Some(Value::Integer(-2)));
        let text = Batch::empty(Schema::new(vec![Field::new("n", DataType::Text, true)]).unwrap());
        assert!(ColumnStatistics::from_batches(&[text], "missing").is_err());
    }
}


#[cfg(test)]
mod observation_atomicity_tests {
    use super::*;

    #[test]
    fn invalid_comparison_preserves_all_statistics() {
        let mut stats = ColumnStatistics {
            rows: 3, nulls: 1,
            min: Some(Value::Integer(1)), max: Some(Value::Integer(4)),
        };
        let before = stats.clone();
        assert!(stats.observe(&Value::Text("bad".into())).is_err());
        assert_eq!(stats, before);
    }

    #[test]
    fn null_observation_overflow_preserves_state() {
        let mut stats = ColumnStatistics {
            rows: u64::MAX, nulls: 2, min: None, max: None,
        };
        let before = stats.clone();
        assert!(stats.observe(&Value::Null).is_err());
        assert_eq!(stats, before);
    }
}

#[cfg(test)]
mod merged_statistics_atomicity_consolidation_tests {
    use super::*;
    #[test]
    fn incompatible_merge_preserves_stats_exactly() {
        let mut first = ColumnStatistics { rows: 2, nulls: 0,
            min: Some(Value::Integer(2)), max: Some(Value::Integer(8)) };
        let other = ColumnStatistics { rows: 1, nulls: 0,
            min: Some(Value::Text("a".into())), max: Some(Value::Text("z".into())) };
        let before = first.clone();
        assert!(first.merge(&other).is_err());
        assert_eq!(first, before);
    }
    #[test]
    fn overflow_does_not_partially_update_extrema() {
        let mut first = ColumnStatistics { rows: u64::MAX, nulls: 0,
            min: Some(Value::Integer(2)), max: Some(Value::Integer(8)) };
        let other = ColumnStatistics { rows: 1, nulls: 0,
            min: Some(Value::Integer(1)), max: Some(Value::Integer(9)) };
        let before = first.clone();
        assert!(first.merge(&other).is_err());
        assert_eq!(first, before);
    }
}

#[cfg(test)]
mod direct_multibatch_stats_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn direct_accumulation_matches_sequential_merge() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batches = (0..100).map(|n| Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(n)], vec![Value::Null], vec![Value::Integer(-n)]
        ]).unwrap()).collect::<Vec<_>>();
        let mut merged = ColumnStatistics::default();
        for batch in &batches { merged.merge(&ColumnStatistics::from_batch(batch, "x").unwrap()).unwrap(); }
        assert_eq!(ColumnStatistics::from_batches(&batches, "x").unwrap(), merged);
    }
}

#[cfg(test)]
mod typed_integer_stats_scan_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn fast_integer_bounds_match_incremental_stats() {
        let schema = Schema::new(vec![
            Field::new("number", DataType::Integer, true),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Null], vec![Value::Integer(i64::MAX)],
            vec![Value::Integer(-5)], vec![Value::Integer(i64::MIN)],
            vec![Value::Null], vec![Value::Integer(15)],
        ]).unwrap();
        let actual = ColumnStatistics::from_batch(&input, "number").unwrap();
        let mut expected = ColumnStatistics::default();
        for value in input.column(0).unwrap() { expected.observe(value).unwrap(); }
        assert_eq!(actual, expected);
        assert_eq!(actual.rows, 6);
        assert_eq!(actual.nulls, 2);
        assert_eq!(actual.min, Some(Value::Integer(i64::MIN)));
        assert_eq!(actual.max, Some(Value::Integer(i64::MAX)));
        let empty = Batch::empty(schema);
        assert_eq!(ColumnStatistics::from_batch(&empty, "number").unwrap(),
            ColumnStatistics::default());
    }
}

#[cfg(test)]
mod merged_batch_stats_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn multi_batch_statistics_match_individual_integer_summaries() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batches = (0..8).map(|index| Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(index * 11)], vec![Value::Null],
            vec![Value::Integer(-index)],
        ]).unwrap()).collect::<Vec<_>>();
        let mut expected = ColumnStatistics::default();
        for batch in &batches {
            expected.merge(&ColumnStatistics::from_batch(batch, "x").unwrap()).unwrap();
        }
        assert_eq!(ColumnStatistics::from_batches(&batches, "x").unwrap(), expected);
    }
}

#[cfg(test)]
mod invalid_input_summary_regressions {
    use super::*;

    #[test]
    fn merge_rejects_invalid_individual_null_counts_without_mutation() {
        let original = ColumnStatistics {
            rows: 100, nulls: 0,
            min: Some(Value::Integer(1)), max: Some(Value::Integer(9)),
        };
        let invalid = ColumnStatistics {
            rows: 1, nulls: 2,
            min: None, max: None,
        };
        let mut left = original.clone();
        assert!(left.merge(&invalid).is_err());
        assert_eq!(left, original);
        // The aggregate NULL count would otherwise be <= the aggregate
        // row count, concealing the impossible input summary.
        let mut malformed = invalid.clone();
        assert!(malformed.merge(&original).is_err());
        assert_eq!(malformed, invalid);
    }
}

#[cfg(test)]
mod typed_double_statistics_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    fn verify(values: Vec<Value>) {
        let schema = Schema::new(vec![Field::new("amount", DataType::Float, true)]).unwrap();
        let input = Batch::from_rows(schema, values.clone().into_iter().map(|v| vec![v]).collect()).unwrap();
        let direct = ColumnStatistics::from_batch(&input, "amount").unwrap();
        let mut incremental = ColumnStatistics::default();
        for value in &values { incremental.observe(value).unwrap(); }
        assert_eq!(direct, incremental);
    }

    #[test]
    fn typed_bounds_match_sql_ordering_with_nan_infinities_and_signed_zero() {
        verify(vec![Value::Null, Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY), Value::Float(-0.0),
            Value::Float(0.0), Value::Float(1.5), Value::Float(f64::INFINITY),
            Value::Float(f64::NAN), Value::Null]);
        verify(vec![Value::Float(0.0), Value::Float(-0.0)]);
        verify(vec![Value::Float(-0.0), Value::Float(0.0)]);
        verify(vec![Value::Float(f64::NAN), Value::Float(f64::NAN)]);
        verify(vec![Value::Null, Value::Null]);
        verify(vec![]);
    }
}

#[cfg(test)]
mod boolean_vector_statistics_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn typed_boolean_bounds_match_incremental_reference() {
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, true)]).unwrap();
        for values in [
            vec![],
            vec![Value::Null, Value::Null],
            vec![Value::Boolean(true), Value::Boolean(true)],
            vec![Value::Boolean(false), Value::Boolean(false)],
            vec![Value::Boolean(true), Value::Null, Value::Boolean(false), Value::Boolean(true)],
        ] {
            let batch = Batch::from_rows(schema.clone(),
                values.iter().cloned().map(|value| vec![value]).collect()).unwrap();
            let summary = ColumnStatistics::from_batch(&batch, "flag").unwrap();
            let mut reference = ColumnStatistics::default();
            for value in &values { reference.observe(value).unwrap(); }
            assert_eq!(summary, reference);
        }
    }

    #[test]
    fn boolean_summaries_merge_without_losing_nulls_or_bounds() {
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, true)]).unwrap();
        let create = |values: Vec<Value>| Batch::from_rows(schema.clone(),
            values.into_iter().map(|v| vec![v]).collect()).unwrap();
        let left = create(vec![Value::Null, Value::Boolean(true)]);
        let right = create(vec![Value::Boolean(false), Value::Null]);
        let all = create(vec![Value::Null, Value::Boolean(true),
            Value::Boolean(false), Value::Null]);
        let mut merged = ColumnStatistics::from_batch(&left, "flag").unwrap();
        merged.merge(&ColumnStatistics::from_batch(&right, "flag").unwrap()).unwrap();
        assert_eq!(merged, ColumnStatistics::from_batch(&all, "flag").unwrap());
        assert_eq!(merged.min, Some(Value::Boolean(false)));
        assert_eq!(merged.max, Some(Value::Boolean(true)));
    }
}

#[cfg(test)]
mod borrowed_text_statistics_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn text_extrema_match_scalar_observation_across_nulls_and_unicode() {
        let schema = Schema::new(vec![Field::new("label", DataType::Text, true)]).unwrap();
        for input in [
            vec![],
            vec![Value::Null, Value::Null],
            vec![
                Value::Text("middle".into()), Value::Text("é".into()),
                Value::Text("a".into()), Value::Text("🙂".into()),
                Value::Null, Value::Text("a".into()),
            ],
            (0..1000).map(|n| Value::Text(format!("{:04}-{}", 999-n, "x".repeat(512)))).collect(),
        ] {
            let batch = Batch::from_rows(schema.clone(),
                input.iter().cloned().map(|value| vec![value]).collect()).unwrap();
            let mut reference = ColumnStatistics::default();
            for value in &input { reference.observe(value).unwrap(); }
            assert_eq!(ColumnStatistics::from_batch(&batch, "label").unwrap(), reference);
        }
    }
}

#[cfg(test)]
mod borrowed_multibatch_text_extrema_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn matches_batchwise_merges_for_wide_nullable_unicode_text() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let batches = vec![
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Text("z".repeat(32768))], vec![Value::Null],
                vec![Value::Text("é".repeat(32768))],
            ]).unwrap(),
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Text("a".repeat(32768))], vec![Value::Text("z".repeat(32768))],
            ]).unwrap(),
            Batch::empty(schema.clone()),
            Batch::from_rows(schema, vec![vec![Value::Null]]).unwrap(),
        ];
        let mut reference = ColumnStatistics::default();
        for batch in &batches {
            reference.merge(&ColumnStatistics::from_batch(batch, "key").unwrap()).unwrap();
        }
        assert_eq!(ColumnStatistics::from_batches(&batches, "key").unwrap(), reference);
        assert_eq!(ColumnStatistics::from_batches(&[], "key").unwrap(), ColumnStatistics::default());
        let mixed = Batch::from_rows(
            Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap();
        assert!(ColumnStatistics::from_batches(&[batches[0].clone(), mixed], "key").is_err());
    }
}

#[cfg(test)]
mod direct_multibatch_integer_stats_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn matches_checked_merges_for_many_nullable_integer_batches() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let batches = (0..128_i64).map(|i| Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(i - 1000)],
            vec![Value::Null],
            vec![Value::Integer(500 - i)],
        ]).unwrap()).collect::<Vec<_>>();
        let mut expected = ColumnStatistics::default();
        for batch in &batches {
            expected.merge(&ColumnStatistics::from_batch(batch, "n").unwrap()).unwrap();
        }
        assert_eq!(ColumnStatistics::from_batches(&batches, "n").unwrap(), expected);
        assert_eq!((expected.rows, expected.nulls), (384, 128));
        assert_eq!(ColumnStatistics::from_batches(&[Batch::empty(schema.clone())],
            "n").unwrap(), ColumnStatistics::default());
        let changed = Batch::empty(Schema::new(vec![
            Field::new("n", DataType::Float, true),
        ]).unwrap());
        assert!(ColumnStatistics::from_batches(&[batches[0].clone(), changed], "n").is_err());
    }
}
