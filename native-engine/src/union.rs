use crate::{batch::Batch, Error, Result};
#[cfg(test)]
use crate::schema::Schema;

pub fn union_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let (left, right, schema) = crate::set_ops::align_set_inputs(left, right)?;
    let count = left
        .num_rows()
        .checked_add(right.num_rows())
        .ok_or_else(|| Error::Execution("UNION row count overflow".into()))?;
    let mut columns = Vec::with_capacity(schema.len());
    for column in 0..schema.len() {
        let mut values = Vec::with_capacity(count);
        values.extend_from_slice(left.column(column)?);
        values.extend_from_slice(right.column(column)?);
        columns.push(values);
    }
    Batch::new_with_row_count(schema, columns, count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field},
    };

    #[test]
    fn union_preserves_duplicates_and_nullable_values() {
        let a = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        )
        .unwrap();
        let b = Batch::from_rows(
            Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(1)]],
        )
        .unwrap();
        let merged = union_all(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.row(1).unwrap(), vec![Value::Null]);
    }
}

 
/// UNION DISTINCT matches columns by position and removes repeated complete rows.
const MAX_UNION_DISTINCT_KEYS: usize = 1_000_000;
const MAX_UNION_DISTINCT_BYTES: usize = 64 * 1024 * 1024;

pub fn union_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    union_distinct_with_budget(
        left, right, MAX_UNION_DISTINCT_KEYS, MAX_UNION_DISTINCT_BYTES,
    )
}

/// Limit retained unique rows independently of the number of source rows.
pub fn union_distinct_with_budget(
    left: &Batch, right: &Batch, max_keys: usize, max_bytes: usize,
) -> Result<Batch> {
    let (left, right, schema) = crate::set_ops::align_set_inputs(left, right)?;
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    let mut retained = 0usize;
    for input in [&*left, &*right] {
        for index in 0..input.num_rows() {
            let row = input.row(index)?;
            if seen.contains(&row) { continue; }
            if seen.len() >= max_keys {
                return Err(Error::Execution("UNION DISTINCT key limit exceeded".into()));
            }
            let base = 64usize.checked_add(schema.len()
                .checked_mul(std::mem::size_of::<crate::scalar::Value>())
                .ok_or_else(|| Error::Execution("UNION DISTINCT budget overflow".into()))?)
                .ok_or_else(|| Error::Execution("UNION DISTINCT budget overflow".into()))?;
            let footprint = row.iter().try_fold(base, |used, value| {
                used.checked_add(match value {
                    crate::scalar::Value::Text(text) => text.len(),
                    _ => 0,
                })
            }).ok_or_else(|| Error::Execution("UNION DISTINCT budget overflow".into()))?;
            retained = retained.checked_add(footprint)
                .filter(|total| *total <= max_bytes)
                .ok_or_else(|| Error::Execution("UNION DISTINCT byte budget exceeded".into()))?;
            seen.insert(row.clone());
            rows.push(row);
        }
    }
    Batch::from_rows(schema, rows)
}

#[cfg(test)]
mod union_distinct_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn distinct_set_union_collapses_null_and_duplicate_rows() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]).unwrap();
        assert_eq!(union_distinct(&a, &b).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]);
    }
}


#[cfg(test)]
mod incremental_union_distinct_regression {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn deduplicates_across_inputs_and_widens_nullability() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("left_id", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Integer(2)],
        ]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("right_id", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null],
            vec![Value::Integer(3)], vec![Value::Integer(3)],
        ]).unwrap();
        let output = union_distinct(&left, &right).unwrap();
        assert_eq!(output.rows().unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(3)],
        ]);
        assert_eq!(output.schema().fields()[0].name, "left_id");
        assert!(output.schema().fields()[0].nullable);
    }
}

#[cfg(test)]
mod zero_column_union_consolidation_tests {
    use super::*;
    #[test]
    fn union_preserves_zero_width_multiset_cardinality() {
        let schema = Schema::new(vec![]).unwrap();
        let a = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let b = Batch::new_with_row_count(schema, vec![], 3).unwrap();
        assert_eq!(union_all(&a, &b).unwrap().rows().unwrap(), vec![vec![]; 5]);
        assert_eq!(union_distinct(&a, &b).unwrap().rows().unwrap(), vec![vec![]]);
    }
}

#[cfg(test)]
mod union_distinct_memory_limits_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn duplicate_rows_do_not_count_twice_against_key_or_byte_budgets() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("hello".into())],
            vec![Value::Text("hello".into())],
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Text("hello".into())],
            vec![Value::Text("world".into())],
        ]).unwrap();
        let size = 64 + std::mem::size_of::<Value>() + "hello".len();
        assert_eq!(union_distinct_with_budget(&a, &b, 1, size).unwrap_err()
            .to_string(), "execution: UNION DISTINCT key limit exceeded");
        assert_eq!(union_distinct_with_budget(&a, &b, 2, size * 2 - 1)
            .unwrap_err().to_string(), "execution: UNION DISTINCT byte budget exceeded");
        let result = union_distinct_with_budget(&a, &b, 2, size * 2).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Text("hello".into())],
            vec![Value::Text("world".into())],
        ]);
    }
}
