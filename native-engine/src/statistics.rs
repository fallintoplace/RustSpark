//! Statistics for cost-based plan selection and row-group pruning.
use std::collections::HashSet;
use crate::{batch::Batch, scalar::Value, Result};

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    pub row_count: usize,
    pub null_count: usize,
    /// None means the tracking budget was exceeded.
    pub distinct_count: Option<usize>,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableStats {
    pub row_count: usize,
    pub columns: Vec<ColumnStats>,
}

/// Distinct tracking stops after max_distinct keys to avoid unbounded state.
/// A NaN value makes the min/max bounds unavailable for safe pruning.
pub fn collect(batch: &Batch, max_distinct: usize) -> Result<TableStats> {
    let mut columns = Vec::with_capacity(batch.schema().len());
    for column in 0..batch.schema().len() {
        let mut distinct = Some(HashSet::new());
        let mut null_count = 0;
        let mut min: Option<Value> = None;
        let mut max: Option<Value> = None;
        let mut comparable = true;
        for value in batch.column(column)? {
            if value.is_null() {
                null_count += 1;
                continue;
            }
            if let Some(seen) = &mut distinct {
                if !seen.contains(value) && seen.len() >= max_distinct {
                    distinct = None;
                } else {
                    seen.insert(value.clone());
                }
            }
            if matches!(value, Value::Float(x) if x.is_nan()) {
                comparable = false;
                min = None;
                max = None;
                continue;
            }
            if !comparable { continue; }
            if min.as_ref().is_none_or(|current| value.sql_compare(current).ok().flatten().is_some_and(|ord| ord.is_lt())) {
                min = Some(value.clone());
            }
            if max.as_ref().is_none_or(|current| value.sql_compare(current).ok().flatten().is_some_and(|ord| ord.is_gt())) {
                max = Some(value.clone());
            }
        }
        columns.push(ColumnStats {
            row_count: batch.num_rows(),
            null_count,
            distinct_count: distinct.map(|set| set.len()),
            min,
            max,
        });
    }
    Ok(TableStats { row_count: batch.num_rows(), columns })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn stats_count_nulls_and_bound_distinct_memory() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(3)],
                 vec![Value::Integer(2)]]
        ).unwrap();
        let s = collect(&batch, 8).unwrap();
        assert_eq!(s.columns[0].null_count, 1);
        assert_eq!(s.columns[0].distinct_count, Some(2));
        assert_eq!(s.columns[0].min, Some(Value::Integer(2)));
        assert_eq!(collect(&batch, 1).unwrap().columns[0].distinct_count, None);
    }

    #[test]
    fn nan_disables_unsafe_min_max_bounds() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Float, true)]).unwrap(),
            vec![vec![Value::Float(3.0)], vec![Value::Float(f64::NAN)]]
        ).unwrap();
        let s = collect(&batch, 10).unwrap();
        assert_eq!(s.columns[0].min, None);
        assert_eq!(s.columns[0].max, None);
    }
}
