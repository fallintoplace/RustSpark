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
        if let Some(value) = &other.min {
            if self.min.as_ref().is_none_or(|current| {
                matches!(value.sql_compare(current), Ok(Some(Ordering::Less)))
            }) {
                self.min = Some(value.clone());
            }
        }
        if let Some(value) = &other.max {
            if self.max.as_ref().is_none_or(|current| {
                matches!(value.sql_compare(current), Ok(Some(Ordering::Greater)))
            }) {
                self.max = Some(value.clone());
            }
        }
        self.rows = rows;
        self.nulls = nulls;
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
