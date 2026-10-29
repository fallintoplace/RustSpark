use std::cmp::Ordering;

use crate::{batch::Batch, scalar::Value, Error, Result};

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

impl ColumnStatistics {
    pub fn from_batch(batch: &Batch, name: &str) -> Result<Self> {
        let column = batch.schema().column(name)?;
        let mut stats = Self::default();
        for value in batch.column(column)? {
            stats.observe(value)?;
        }
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
        // Compare both extrema before mutating counters or bounds.
        let replace_min = !value.is_null() && match self.min.as_ref() {
            Some(current) => matches!(value.sql_compare(current)?, Some(Ordering::Less)),
            None => true,
        };
        let replace_max = !value.is_null() && match self.max.as_ref() {
            Some(current) => matches!(value.sql_compare(current)?, Some(Ordering::Greater)),
            None => true,
        };
        self.rows = rows;
        self.nulls = nulls;
        if replace_min { self.min = Some(value.clone()); }
        if replace_max { self.max = Some(value.clone()); }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        let rows = self.rows.checked_add(other.rows)
            .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
        let nulls = self.nulls.checked_add(other.nulls)
            .ok_or_else(|| Error::Execution("statistics NULL count overflow".into()))?;
        if nulls > rows {
            return Err(Error::Execution("invalid NULL count".into()));
        }
        // Validate both extrema before committing counters or bounds.
        let mut min = self.min.clone();
        let mut max = self.max.clone();
        if let Some(value) = &other.min {
            if let Some(current) = &min {
                if matches!(value.sql_compare(current)?, Some(Ordering::Less)) {
                    min = Some(value.clone());
                }
            } else {
                min = Some(value.clone());
            }
        }
        if let Some(value) = &other.max {
            if let Some(current) = &max {
                if matches!(value.sql_compare(current)?, Some(Ordering::Greater)) {
                    max = Some(value.clone());
                }
            } else {
                max = Some(value.clone());
            }
        }
        self.rows = rows;
        self.nulls = nulls;
        self.min = min;
        self.max = max;
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
        let mut combined = Self::default();
        let mut expected_type = None;
        for batch in batches {
            let field = batch.schema().field(batch.schema().column(name)?)?;
            if expected_type.is_some_and(|kind| kind != field.data_type) {
                return Err(Error::Type("statistics batches disagree on column type".into()));
            }
            expected_type = Some(field.data_type);
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
mod selective_extrema_perf_tests {
    use super::*;
    #[test]
    fn stable_text_extrema_remain_unchanged_on_inner_values() {
        let mut stats = ColumnStatistics::default();
        for text in ["alpha", "zulu", "mike", "mid", "beta"] {
            stats.observe(&Value::Text(text.to_string())).unwrap();
        }
        assert_eq!(stats.rows, 5);
        assert_eq!(stats.min, Some(Value::Text("alpha".into())));
        assert_eq!(stats.max, Some(Value::Text("zulu".into())));
        let old = stats.clone();
        assert!(stats.observe(&Value::Integer(100)).is_err());
        assert_eq!(stats, old);
    }
}
