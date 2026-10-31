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
    batch.take(&distinct_indices(batch, max_keys, max_bytes)?)
}

/// Deduplicate an owned input while moving retained TEXT values.
pub fn distinct_owned(batch: Batch) -> Result<Batch> {
    distinct_owned_with_budget(batch, 1_000_000, usize::MAX)
}

/// The same key/byte admission rules apply to borrowed and owned DISTINCT.
pub fn distinct_owned_with_budget(
    batch: Batch, max_keys: usize, max_bytes: usize,
) -> Result<Batch> {
    let selected = distinct_indices(&batch, max_keys, max_bytes)?;
    if selected.len() == batch.num_rows() { return Ok(batch); }
    batch.take_sorted_owned(&selected)
}

fn distinct_indices(batch: &Batch, max_keys: usize, max_bytes: usize) -> Result<Vec<usize>> {
    if batch.schema().is_empty() {
        if batch.num_rows() == 0 { return Ok(Vec::new()); }
        if max_keys == 0 {
            return Err(crate::Error::Execution("DISTINCT key limit exceeded".into()));
        }
        if max_bytes < 64 {
            return Err(crate::Error::Execution("DISTINCT byte budget exceeded".into()));
        }
        // Every row has the same empty key. Record only its first offset.
        return Ok(vec![0]);
    }
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
    Ok(indices)
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
    distinct_on_with_budget(batch, keys, usize::MAX, usize::MAX)
}

/// Select the first row for each key tuple with distinct-key and estimated
/// in-memory byte budgets. Borrowed keys avoid cloning variable-width values.
pub fn distinct_on_with_budget(
    batch: &Batch, keys: &[&str], max_keys: usize, max_bytes: usize,
) -> Result<Batch> {
    if keys.is_empty() {
        return Err(crate::Error::Schema("DISTINCT ON requires key columns".into()));
    }
    let indices = keys.iter().map(|key| batch.schema().column(key))
        .collect::<Result<Vec<_>>>()?;
    let mut unique = HashSet::with_capacity(indices.len());
    if indices.iter().any(|&index| !unique.insert(index)) {
        return Err(crate::Error::Schema("DISTINCT ON key columns must be unique".into()));
    }
    let columns = indices.iter().map(|&index| batch.column(index))
        .collect::<Result<Vec<_>>>()?;
    let base = 64usize.checked_add(columns.len()
        .checked_mul(std::mem::size_of::<crate::scalar::Value>())
        .ok_or_else(|| crate::Error::Execution("DISTINCT ON byte budget overflow".into()))?)
        .ok_or_else(|| crate::Error::Execution("DISTINCT ON byte budget overflow".into()))?;
    let footprint = |row: usize| -> Result<usize> {
        columns.iter().try_fold(base, |total, column| {
            let payload = match &column[row] {
                crate::scalar::Value::Text(text) => text.len(),
                _ => 0,
            };
            total.checked_add(payload)
                .ok_or_else(|| crate::Error::Execution("DISTINCT ON byte budget overflow".into()))
        })
    };
    let mut bytes = 0usize;
    let mut selected = Vec::new();
    if columns.len() == 1 {
        // Probe each borrowed key once, including newly admitted values.
        let mut seen = HashMap::<&crate::scalar::Value, ()>::new();
        for (row, value) in columns[0].iter().enumerate() {
            match seen.entry(value) {
                Entry::Occupied(_) => {}
                Entry::Vacant(entry) => {
                    if selected.len() >= max_keys {
                        return Err(crate::Error::Execution("DISTINCT ON key budget exceeded".into()));
                    }
                    bytes = bytes.checked_add(footprint(row)?)
                        .filter(|&total| total <= max_bytes)
                        .ok_or_else(|| crate::Error::Execution("DISTINCT ON byte budget exceeded".into()))?;
                    entry.insert(());
                    selected.push(row);
                }
            }
        }
    } else {
        let mut seen = HashMap::<BorrowedRow<'_>, ()>::new();
        for row in 0..batch.num_rows() {
            let key = BorrowedRow { columns: &columns, row };
            let at_limit = seen.len() >= max_keys;
            match seen.entry(key) {
                Entry::Occupied(_) => {}
                Entry::Vacant(entry) => {
                    if at_limit {
                        return Err(crate::Error::Execution("DISTINCT ON key budget exceeded".into()));
                    }
                    bytes = bytes.checked_add(footprint(row)?)
                        .filter(|&total| total <= max_bytes)
                        .ok_or_else(|| crate::Error::Execution("DISTINCT ON byte budget exceeded".into()))?;
                    entry.insert(());
                    selected.push(row);
                }
            }
        }
    }
    batch.take(&selected)
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
    use crate::scalar::Value;
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

#[cfg(test)]
mod distinct_on_duplicate_columns_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn rejects_repeated_keys_even_with_different_case() {
        let schema = Schema::new(vec![Field::new("Id", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![crate::scalar::Value::Integer(1)]]).unwrap();
        assert!(distinct_on(&batch, &["Id", "id"]).is_err());
        assert_eq!(distinct_on(&batch, &["id"]).unwrap().num_rows(), 1);
    }
}

#[cfg(test)]
mod zero_column_distinct_fast_path_tests {
    use super::*;
    use crate::schema::Schema;
    #[test]
    fn deduplicates_wide_empty_schemas_without_row_hashing() {
        let schema = Schema::new(vec![]).unwrap();
        let wide = Batch::new_with_row_count(schema.clone(), vec![], 100_000).unwrap();
        assert_eq!(distinct_with_budget(&wide, 1, 64).unwrap().num_rows(), 1);
        assert!(distinct_with_budget(&wide, 0, 64).is_err());
        assert!(distinct_with_budget(&wide, 1, 63).is_err());
        let empty = Batch::empty(schema);
        assert_eq!(distinct_with_budget(&empty, 0, 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod bounded_borrowed_distinct_on_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn repeated_long_composite_keys_consume_only_distinct_budget() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Text, true),
            Field::new("version", DataType::Float, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let name = "é🙂".repeat(2048);
        let data = Batch::from_rows(schema, vec![
            vec![Value::Text(name.clone()), Value::Float(-0.0), Value::Text("a".into())],
            vec![Value::Text(name.clone()), Value::Float(0.0), Value::Text("dup".into())],
            vec![Value::Text(name.clone()), Value::Float(f64::NAN), Value::Text("b".into())],
            vec![Value::Text(name.clone()), Value::Float(f64::NAN), Value::Text("dup2".into())],
            vec![Value::Null, Value::Null, Value::Text("c".into())],
        ]).unwrap();
        let single = 64 + 2 * std::mem::size_of::<Value>() + name.len();
        let max_bytes = 2 * single + 64 + 2 * std::mem::size_of::<Value>();
        let result = distinct_on_with_budget(&data, &["name", "version"], 3, max_bytes)
            .unwrap();
        assert_eq!(result.column(2).unwrap(), &[
            Value::Text("a".into()), Value::Text("b".into()), Value::Text("c".into()),
        ]);
        assert!(distinct_on_with_budget(&data, &["name", "version"], 2, max_bytes).is_err());
        assert!(distinct_on_with_budget(&data, &["name", "version"], 3, max_bytes - 1).is_err());
        assert_eq!(result, distinct_on(&data, &["name", "version"]).unwrap());
    }

    #[test]
    fn key_admission_checks_only_unique_single_column_values() {
        let data = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let base = 64 + std::mem::size_of::<Value>();
        assert_eq!(distinct_on_with_budget(&data, &["key"], 2, base * 2)
            .unwrap().num_rows(), 2);
        assert!(distinct_on_with_budget(&data, &["key"], 1, base * 2).is_err());
        assert!(distinct_on_with_budget(&data, &["key"], 2, base * 2 - 1).is_err());
        assert!(distinct_on_with_budget(&data, &["key", "KEY"], 5, usize::MAX).is_err());
        assert_eq!(distinct_on_with_budget(&Batch::empty(data.schema().clone()),
            &["key"], 0, 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod owned_distinct_gather_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn moves_only_first_unique_wide_text_rows_without_new_allocations() {
        let large = "東京🙂".repeat(4096);
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("id", DataType::Float, true),
        ]).unwrap(), vec![
            vec![Value::Text(large.clone()), Value::Float(-0.0)],
            vec![Value::Text(large.clone()), Value::Float(0.0)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Text("different".into()), Value::Null],
        ]).unwrap();
        let ptr = match batch.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(), _ => unreachable!(),
        };
        let expected = distinct(&batch).unwrap();
        let actual = distinct_owned(batch).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.num_rows(), 3);
        assert_eq!(match actual.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(), _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn owned_limits_match_borrowed_and_empty_schema_semantics() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]).unwrap();
        assert_eq!(distinct_owned_with_budget(batch.clone(), 2, usize::MAX).unwrap(),
            distinct_with_budget(&batch, 2, usize::MAX).unwrap());
        assert!(distinct_owned_with_budget(batch.clone(), 1, usize::MAX).is_err());
        assert!(distinct_owned_with_budget(batch, 2, 0).is_err());
        let empty = Schema::new(vec![]).unwrap();
        let repeated = Batch::new_with_row_count(empty, vec![], 100).unwrap();
        assert_eq!(distinct_owned(repeated.clone()).unwrap(),
            distinct(&repeated).unwrap());
        assert!(distinct_owned_with_budget(repeated, 0, 64).is_err());
    }
}

#[cfg(test)]
mod single_probe_distinct_on_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn keeps_first_nullable_text_key_and_preserves_budget_admission() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("payload", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("🙂".into()), Value::Integer(1)],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Text("🙂".into()), Value::Integer(3)],
            vec![Value::Null, Value::Integer(4)],
            vec![Value::Text("é".into()), Value::Integer(5)],
        ]).unwrap();
        let expected = input.take(&[0, 1, 4]).unwrap();
        assert_eq!(distinct_on_with_budget(&input, &["key"], 3, usize::MAX).unwrap(), expected);
        assert!(distinct_on_with_budget(&input, &["key"], 2, usize::MAX).is_err());
        assert!(distinct_on_with_budget(&input, &["key"], 3, 0).is_err());
        let only_duplicates = input.take(&[0, 2]).unwrap();
        assert_eq!(distinct_on_with_budget(&only_duplicates, &["key"], 1, usize::MAX)
            .unwrap().num_rows(), 1);
    }
}
