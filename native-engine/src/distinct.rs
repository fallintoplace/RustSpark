use std::collections::HashSet;

use crate::{batch::Batch, Result};

/// Keep the first occurrence of each complete row. NULL values are equal for
/// DISTINCT/grouping, unlike WHERE's three-valued comparison semantics.
pub fn distinct(batch: &Batch) -> Result<Batch> {
    distinct_with_limit(batch, 1_000_000)
}

/// Deduplicate complete rows with an explicit key-cardinality admission limit.
pub fn distinct_with_limit(batch: &Batch, max_keys: usize) -> Result<Batch> {
    let mut seen = HashSet::new();
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        let key = batch.row(row)?;
        if !seen.contains(&key) {
            if seen.len() >= max_keys {
                return Err(crate::Error::Execution("DISTINCT key limit exceeded".into()));
            }
            seen.insert(key);
            indices.push(row);
        }
    }
    batch.take(&indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn duplicate_nulls_and_signed_zero_collapse() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Float, true),
        ])
        .unwrap();
        let batch = Batch::from_rows(
            schema,
            vec![
                vec![Value::Null],
                vec![Value::Null],
                vec![Value::Float(-0.0)],
                vec![Value::Float(0.0)],
            ],
        )
        .unwrap();
        assert_eq!(distinct(&batch).unwrap().num_rows(), 2);
    }
}

 
/// Keep the first input row for each distinct combination of key columns.
/// NULL and signed zero compare equal as grouping keys.
pub fn distinct_on(batch: &Batch, keys: &[&str]) -> Result<Batch> {
    if keys.is_empty() {
        return Err(crate::Error::Schema("DISTINCT ON requires key columns".into()));
    }
    let columns = keys.iter().map(|key| batch.schema().column(key))
        .collect::<Result<Vec<_>>>()?;
    let mut seen = HashSet::new();
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        let key = columns.iter().map(|&column| batch.value(row, column).cloned())
            .collect::<Result<Vec<_>>>()?;
        if seen.insert(key) {
            indices.push(row);
        }
    }
    batch.take(&indices)
}

#[cfg(test)]
mod distinct_on_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn keeps_first_rows_with_nullable_keys() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap(), vec![
            vec![Value::Null, Value::Text("first".into())],
            vec![Value::Null, Value::Text("second".into())],
            vec![Value::Integer(2), Value::Text("third".into())],
        ]).unwrap();
        let result = distinct_on(&input, &["id"]).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.row(0).unwrap()[1], Value::Text("first".into()));
        assert!(distinct_on(&input, &[]).is_err());
        assert!(distinct_on(&input, &["unknown"]).is_err());
    }
    #[test]
    fn bounded_distinct_counts_unique_keys_not_source_rows() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(1)],
                 vec![Value::Integer(2)], vec![Value::Integer(3)]],
        ).unwrap();
        assert_eq!(distinct_with_limit(&batch, 2).unwrap_err().to_string(),
            "execution: DISTINCT key limit exceeded");
        assert_eq!(distinct_with_limit(&batch, 3).unwrap().num_rows(), 3);
    }
}
