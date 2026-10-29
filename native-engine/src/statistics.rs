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
        self.rows = self.rows.checked_add(1)
            .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
        if value.is_null() {
            self.nulls += 1;
            return Ok(());
        }
        if let Some(current) = &self.min {
            if matches!(value.sql_compare(current)?, Some(Ordering::Less)) {
                self.min = Some(value.clone());
            }
        } else {
            self.min = Some(value.clone());
        }
        if let Some(current) = &self.max {
            if matches!(value.sql_compare(current)?, Some(Ordering::Greater)) {
                self.max = Some(value.clone());
            }
        } else {
            self.max = Some(value.clone());
        }
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
        let mut min = self.min.clone();
        let mut max = self.max.clone();
        if let Some(value) = &other.min {
            if min.as_ref().is_none_or(|current|
                matches!(value.sql_compare(current), Ok(Some(Ordering::Less)))) {
                // Check compatible types even when the new candidate is not smaller.
                min = Some(value.clone());
            } else if let Some(current) = &min {
                value.sql_compare(current)?;
            }
        }
        if let Some(value) = &other.max {
            if max.as_ref().is_none_or(|current|
                matches!(value.sql_compare(current), Ok(Some(Ordering::Greater)))) {
                max = Some(value.clone());
            } else if let Some(current) = &max {
                value.sql_compare(current)?;
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
mod merge_validation_tests {
    use super::*;

    #[test]
    fn incompatible_extrema_do_not_commit_partial_merges() {
        let mut left = ColumnStatistics {
            rows: 1, nulls: 0, min: Some(Value::Integer(4)), max: Some(Value::Integer(4)),
        };
        let right = ColumnStatistics {
            rows: 1, nulls: 0, min: Some(Value::Text("x".into())), max: Some(Value::Text("x".into())),
        };
        let original = left.clone();
        assert!(left.merge(&right).is_err());
        assert_eq!(left, original);
    }
}
