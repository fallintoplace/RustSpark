use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::hash::{Hash, Hasher};

use crate::{batch::Batch, Result};

/// Hash and compare complete native rows without owning cell values.
#[derive(Clone, Copy)]
struct BorrowedRow<'a> {
    columns: &'a [&'a [crate::scalar::Value]],
    row: usize,
}

impl Hash for BorrowedRow<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.columns.len().hash(state);
        for column in self.columns { column[self.row].hash(state); }
    }
}

impl PartialEq for BorrowedRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len() &&
            self.columns.iter().zip(other.columns)
                .all(|(left, right)| left[self.row] == right[other.row])
    }
}

impl Eq for BorrowedRow<'_> {}

/// Keep the first occurrence of each complete row. NULL values are equal for
/// DISTINCT/grouping, unlike WHERE's three-valued comparison semantics.
pub fn distinct(batch: &Batch) -> Result<Batch> {
    distinct_with_limit(batch, 1_000_000)
}

/// Deduplicate complete rows with an explicit key-cardinality admission limit.
pub fn distinct_with_limit(batch: &Batch, max_keys: usize) -> Result<Batch> {
    distinct_with_budget(batch, max_keys, usize::MAX)
}

/// Deduplicate with independent key-cardinality and estimated byte budgets.
pub fn distinct_with_budget(batch: &Batch, max_keys: usize, max_bytes: usize) -> Result<Batch> {
    let columns = (0..batch.schema().len())
        .map(|index| batch.column(index)).collect::<Result<Vec<_>>>()?;
    let mut seen = HashMap::<BorrowedRow<'_>, ()>::new();
    let mut used = 0usize;
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        let key = BorrowedRow { columns: &columns, row };
        let at_limit = seen.len() >= max_keys;
        match seen.entry(key) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                if at_limit {
                    return Err(crate::Error::Execution("DISTINCT key limit exceeded".into()));
                }
                let base = 64usize.checked_add(columns.len()
                    .checked_mul(std::mem::size_of::<crate::scalar::Value>())
                    .ok_or_else(|| crate::Error::Execution("DISTINCT byte budget overflow".into()))?)
                    .ok_or_else(|| crate::Error::Execution("DISTINCT byte budget overflow".into()))?;
                let footprint = columns.iter().try_fold(base, |total, column|
                    total.checked_add(match &column[row] {
                        crate::scalar::Value::Text(text) => text.len(),
                        _ => 0,
                    })).ok_or_else(|| crate::Error::Execution("DISTINCT byte budget overflow".into()))?;
                used = used.checked_add(footprint)
                    .filter(|bytes| *bytes <= max_bytes)
                    .ok_or_else(|| crate::Error::Execution("DISTINCT byte budget exceeded".into()))?;
                entry.insert(());
                indices.push(row);
            }
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
    if columns.len() == 1 {
        let column = batch.column(columns[0])?;
        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        for (row, value) in column.iter().enumerate() {
            if seen.insert(value) {
                indices.push(row);
            }
        }
        return batch.take(&indices);
    }
    let mut seen = HashSet::new();
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        // Keep keys borrowed from the immutable batch instead of cloning text values
        // for every input row. Only selected output rows are copied by `take`.
        let key = columns.iter().map(|&column| batch.value(row, column))
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

#[cfg(test)]
mod borrowed_multicolumn_distinct_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn multi_column_keys_preserve_first_rows_with_long_text_and_nulls() {
        let schema = Schema::new(vec![
            Field::new("group", DataType::Text, true),
            Field::new("version", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let long = "é🙂".repeat(2048);
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text(long.clone()), Value::Null, Value::Text("first".into())],
            vec![Value::Text(long.clone()), Value::Null, Value::Text("duplicate".into())],
            vec![Value::Text(long.clone()), Value::Integer(1), Value::Text("other".into())],
            vec![Value::Null, Value::Integer(1), Value::Text("null key".into())],
            vec![Value::Null, Value::Integer(1), Value::Text("duplicate null key".into())],
        ]).unwrap();
        let result = distinct_on(&batch, &["group", "version"]).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.column(2).unwrap(), &[
            Value::Text("first".into()),
            Value::Text("other".into()),
            Value::Text("null key".into()),
        ]);
    }
}

#[cfg(test)]
mod bounded_distinct_text_memory_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn duplicate_long_strings_use_only_one_key_budget() {
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let text = "é".repeat(500);
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text(text.clone())],
            vec![Value::Text(text.clone())],
            vec![Value::Null],
        ]).unwrap();
        let per_key = 64 + std::mem::size_of::<Value>();
        assert_eq!(distinct_with_budget(&batch, 2, 2 * per_key + text.len())
            .unwrap().num_rows(), 2);
        assert!(distinct_with_budget(&batch, 2, per_key).is_err());
        assert_eq!(distinct_with_limit(&batch, 2).unwrap().num_rows(), 2);
    }
}

#[cfg(test)]
mod borrowed_full_row_distinct_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn repeated_large_text_rows_and_zero_width_keys_preserve_first_order() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Text, true),
            Field::new("b", DataType::Integer, false),
        ]).unwrap();
        let big = Value::Text("étext".repeat(1024));
        let batch = Batch::from_rows(schema, vec![
            vec![big.clone(), Value::Integer(2)],
            vec![big.clone(), Value::Integer(2)],
            vec![Value::Null, Value::Integer(1)],
            vec![big, Value::Integer(3)],
        ]).unwrap();
        let result = distinct_with_budget(&batch, 3, 64 * 1024).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.row(1).unwrap(), vec![Value::Null, Value::Integer(1)]);
        assert!(distinct_with_budget(&batch, 2, 64 * 1024).is_err());
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        assert_eq!(distinct(&zero).unwrap().num_rows(), 1);
    }
}
