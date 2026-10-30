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

    /// Collect named column summaries for a native batch, including empty columns.
    /// The result is directly consumable by multi-column predicate pruning.
    pub fn all_columns(batch: &Batch) -> Result<std::collections::HashMap<String, Self>> {
        let mut columns = std::collections::HashMap::with_capacity(batch.schema().len());
        for (index, field) in batch.schema().fields().iter().enumerate() {
            let mut stats = Self::default();
            for value in batch.column(index)? {
                stats.observe(value)?;
            }
            columns.insert(field.name.clone(), stats);
        }
        Ok(columns)
    }

    /// Merge complete column statistics over compatible native batches.
    pub fn all_columns_batches(
        batches: &[Batch],
    ) -> Result<std::collections::HashMap<String, Self>> {
        let Some(first) = batches.first() else {
            return Ok(std::collections::HashMap::new());
        };
        let schema = first.schema();
        let mut combined = Self::all_columns(first)?;
        for batch in &batches[1..] {
            if batch.schema() != schema {
                return Err(Error::Schema("statistics input batch schemas differ".into()));
            }
            let next = Self::all_columns(batch)?;
            for (name, stats) in next {
                combined.get_mut(&name)
                    .ok_or_else(|| Error::Schema("missing statistics column".into()))?
                    .merge(&stats)?;
            }
        }
        Ok(combined)
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
        let mut replace_min = false;
        let mut replace_max = false;
        if !value.is_null() {
            replace_min = match &self.min {
                Some(current) => matches!(value.sql_compare(current)?, Some(Ordering::Less)),
                None => true,
            };
            replace_max = match &self.max {
                Some(current) => matches!(value.sql_compare(current)?, Some(Ordering::Greater)),
                None => true,
            };
        }
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
        let rows = self.rows.checked_add(other.rows)
            .ok_or_else(|| Error::Execution("statistics row count overflow".into()))?;
        let nulls = self.nulls.checked_add(other.nulls)
            .ok_or_else(|| Error::Execution("statistics NULL count overflow".into()))?;
        if nulls > rows {
            return Err(Error::Execution("invalid NULL count".into()));
        }
        // Validate both extrema before committing counters or bounds.
        let replace_min = match (&self.min, &other.min) {
            (Some(current), Some(value)) =>
                matches!(value.sql_compare(current)?, Some(Ordering::Less)),
            (None, Some(_)) => true,
            _ => false,
        };
        let replace_max = match (&self.max, &other.max) {
            (Some(current), Some(value)) =>
                matches!(value.sql_compare(current)?, Some(Ordering::Greater)),
            (None, Some(_)) => true,
            _ => false,
        };
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
        let mut combined = Self::default();
        let mut expected_type = None;
        for batch in batches {
            let column = batch.schema().column(name)?;
            let field = batch.schema().field(column)?;
            if expected_type.is_some_and(|kind| kind != field.data_type) {
                return Err(Error::Type("statistics batches disagree on column type".into()));
            }
            expected_type = Some(field.data_type);
            for value in batch.column(column)? {
                combined.observe(value)?;
            }
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
mod all_native_column_statistics_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, pruning,
        schema::{DataType, Field, Schema}};

    #[test]
    fn summaries_support_multicolumn_pruning() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(10), Value::Null],
            vec![Value::Integer(20), Value::Text("a".into())],
        ]).unwrap();
        let stats = ColumnStatistics::all_columns(&batch).unwrap();
        assert_eq!(stats["id"].rows, 2);
        assert_eq!(stats["label"].nulls, 1);
        let predicate = Expr::binary(Expr::Column("id".into()),
            BinaryOp::Greater, Expr::Literal(Value::Integer(25)));
        assert!(pruning::can_skip_columns(&stats, &predicate));
        let empty = ColumnStatistics::all_columns(&Batch::empty(schema)).unwrap();
        assert_eq!(empty["label"].rows, 0);
    }
}

#[cfg(test)]
mod native_multi_column_batch_statistics_tests {
    use super::*;
    use crate::{expr::{BinaryOp, Expr}, pruning,
        schema::{DataType, Field, Schema}};

    #[test]
    fn merges_nulls_ranges_and_types_across_boundaries() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(5), Value::Null],
            vec![Value::Null, Value::Text("z".into())],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(15), Value::Text("a".into())],
        ]).unwrap();
        let merged = ColumnStatistics::all_columns_batches(&[left, right]).unwrap();
        assert_eq!(merged["id"].rows, 3);
        assert_eq!(merged["id"].nulls, 1);
        assert_eq!(merged["id"].min, Some(Value::Integer(5)));
        assert_eq!(merged["id"].max, Some(Value::Integer(15)));
        let predicate = Expr::binary(Expr::Column("id".into()),
            BinaryOp::Greater, Expr::Literal(Value::Integer(20)));
        assert!(pruning::can_skip_columns(&merged, &predicate));
        assert!(ColumnStatistics::all_columns_batches(&[
            Batch::empty(schema),
            Batch::empty(Schema::new(vec![Field::new("id", DataType::Text, true)]).unwrap()),
        ]).is_err());
        assert!(ColumnStatistics::all_columns_batches(&[]).unwrap().is_empty());
    }
}
