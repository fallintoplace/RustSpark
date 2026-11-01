//! Native SQL set operators with explicit schema compatibility checks.
use std::{borrow::Cow, collections::{HashMap, HashSet}, hash::{Hash, Hasher}};
use crate::{
    batch::Batch, scalar::Value, schema::{DataType, Field, Schema}, Error, Result,
};

pub fn compatible_schema(left: &Batch, right: &Batch) -> Result<Schema> {
    if left.schema().len() != right.schema().len() {
        return Err(Error::Schema("set operation column counts must match".into()));
    }
    let fields = left.schema().fields().iter().zip(right.schema().fields()).map(|(a, b)| {
        let kind = match (a.data_type, b.data_type) {
            (left, right) if left == right => left,
            (DataType::Integer, DataType::Float) |
            (DataType::Float, DataType::Integer) => DataType::Float,
            _ => return Err(Error::Type(format!(
                "set operation type mismatch at {}", a.name
            ))),
        };
        Ok(Field::new(&a.name, kind, a.nullable || b.nullable))
    }).collect::<Result<Vec<_>>>()?;
    Schema::new(fields)
}

/// Widen numeric inputs before equality/hash operations. Without this, a
/// numerically equal INT and DOUBLE are different native hash keys.
pub fn align_set_inputs<'a>(
    left: &'a Batch, right: &'a Batch,
) -> Result<(Cow<'a, Batch>, Cow<'a, Batch>, Schema)> {
    let schema = compatible_schema(left, right)?;
    let align = |batch: &'a Batch| -> Result<Cow<'a, Batch>> {
        let unchanged = schema.fields().iter().zip(batch.schema().fields())
            .all(|(expected, actual)| expected.data_type == actual.data_type);
        if unchanged { return Ok(Cow::Borrowed(batch)); }
        let mut columns = Vec::with_capacity(schema.len());
        for (i, expected) in schema.fields().iter().enumerate() {
            let values = batch.column(i)?.iter().map(|value| {
                match (expected.data_type, value) {
                    (DataType::Float, Value::Integer(n)) => Value::Float(*n as f64),
                    _ => value.clone(),
                }
            }).collect::<Vec<_>>();
            columns.push(values);
        }
        // The input already passed its original schema. The only permitted
        // conversion above is INTEGER -> FLOAT, so all output cells match
        // the widened schema without another row-by-row validation pass.
        Ok(Cow::Owned(Batch::from_validated_columns(
            schema.clone(), columns, batch.num_rows(),
        )?))
    };
    Ok((align(left)?, align(right)?, schema))
}

const MAX_DISTINCT_SET_KEYS: usize = 1_000_000;

/// A multi-column set key borrowing validated batch buffers. Comparison and
/// hashing normalize SQL NULLs, signed zero and NaN through Value equality.
#[derive(Clone, Copy)]
struct BorrowedSetRow<'a> {
    columns: &'a [&'a [Value]],
    row: usize,
}

impl Hash for BorrowedSetRow<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.columns.len().hash(state);
        for column in self.columns { column[self.row].hash(state); }
    }
}

impl PartialEq for BorrowedSetRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len() &&
            self.columns.iter().zip(other.columns)
                .all(|(left, right)| left[self.row] == right[other.row])
    }
}
impl Eq for BorrowedSetRow<'_> {}

fn borrowed_columns(batch: &Batch) -> Result<Vec<&[Value]>> {
    (0..batch.schema().len())
        .map(|index| batch.column(index)).collect()
}

fn bounded_right_keys<'a>(
    columns: &'a [&'a [Value]], rows: usize, max_keys: usize,
) -> Result<HashSet<BorrowedSetRow<'a>>> {
    let mut keys = HashSet::new();
    for row in 0..rows {
        let key = BorrowedSetRow { columns, row };
        if keys.len() >= max_keys && !keys.contains(&key) {
            return Err(Error::Execution("set operation distinct-key budget exceeded".into()));
        }
        keys.insert(key);
    }
    Ok(keys)
}

pub fn intersect_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.num_rows() == 0 || right.num_rows() == 0 {
        return Ok(Batch::empty(compatible_schema(left, right)?));
    }
    let (left, right, schema) = align_set_inputs(left, right)?;
    if schema.is_empty() {
        return Batch::new_with_row_count(schema, vec![], 1);
    }
    // When the left side is smaller, hash its keys and remove every key
    // present on the right. This bounds the hash state by the left cardinality
    // while retaining first-appearance order when emitting the left rows.
    if left.num_rows() < right.num_rows() {
        let left_columns = borrowed_columns(&left)?;
        let right_columns = borrowed_columns(&right)?;
        let mut unmatched = bounded_right_keys(
            &left_columns, left.num_rows(), MAX_DISTINCT_SET_KEYS,
        )?;
        for row in 0..right.num_rows() {
            unmatched.remove(&BorrowedSetRow { columns: &right_columns, row });
            if unmatched.is_empty() { break; }
        }
        let mut emitted = HashSet::new();
        let mut selected = Vec::new();
        for row in 0..left.num_rows() {
            let key = BorrowedSetRow { columns: &left_columns, row };
            if !unmatched.contains(&key) && emitted.insert(key) {
                selected.push(row);
            }
        }
        return left.take(&selected)?.relabel(schema);
    }
    if schema.len() == 1 {
        let mut right_values = HashSet::<&Value>::new();
        for value in right.column(0)? {
            if right_values.len() >= MAX_DISTINCT_SET_KEYS && !right_values.contains(value) {
                return Err(Error::Execution("set operation distinct-key budget exceeded".into()));
            }
            right_values.insert(value);
        }
        let mut emitted = HashSet::<&Value>::new();
        let mut rows = Vec::new();
        for (index, value) in left.column(0)?.iter().enumerate() {
            if right_values.contains(value) && emitted.insert(value) { rows.push(index); }
        }
        return left.take(&rows)?.relabel(schema);
    }
    let left_columns = borrowed_columns(&left)?;
    let right_columns = borrowed_columns(&right)?;
    let right_rows = bounded_right_keys(&right_columns, right.num_rows(), MAX_DISTINCT_SET_KEYS)?;
    let mut emitted = HashSet::new();
    let mut selected = Vec::new();
    for row in 0..left.num_rows() {
        let key = BorrowedSetRow { columns: &left_columns, row };
        if right_rows.contains(&key) && emitted.insert(key) {
            selected.push(row);
        }
    }
    left.take(&selected)?.relabel(schema)
}

pub fn except_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.num_rows() == 0 {
        return Ok(Batch::empty(compatible_schema(left, right)?));
    }
    let (left, right, schema) = align_set_inputs(left, right)?;
    if schema.is_empty() {
        return Batch::new_with_row_count(schema, vec![], usize::from(right.num_rows() == 0));
    }
    // When the left side is smaller, hash its keys and remove every key
    // present on the right. This bounds the hash state by the left cardinality
    // while retaining first-appearance order when emitting the left rows.
    if left.num_rows() < right.num_rows() {
        let left_columns = borrowed_columns(&left)?;
        let right_columns = borrowed_columns(&right)?;
        let mut unmatched = bounded_right_keys(
            &left_columns, left.num_rows(), MAX_DISTINCT_SET_KEYS,
        )?;
        for row in 0..right.num_rows() {
            unmatched.remove(&BorrowedSetRow { columns: &right_columns, row });
            if unmatched.is_empty() { break; }
        }
        let mut emitted = HashSet::new();
        let mut selected = Vec::new();
        for row in 0..left.num_rows() {
            let key = BorrowedSetRow { columns: &left_columns, row };
            if unmatched.contains(&key) && emitted.insert(key) {
                selected.push(row);
            }
        }
        return left.take(&selected)?.relabel(schema);
    }
    if schema.len() == 1 {
        let mut right_values = HashSet::<&Value>::new();
        for value in right.column(0)? {
            if right_values.len() >= MAX_DISTINCT_SET_KEYS && !right_values.contains(value) {
                return Err(Error::Execution("set operation distinct-key budget exceeded".into()));
            }
            right_values.insert(value);
        }
        let mut emitted = HashSet::<&Value>::new();
        let mut rows = Vec::new();
        for (index, value) in left.column(0)?.iter().enumerate() {
            if !right_values.contains(value) && emitted.insert(value) { rows.push(index); }
        }
        return left.take(&rows)?.relabel(schema);
    }
    let left_columns = borrowed_columns(&left)?;
    let right_columns = borrowed_columns(&right)?;
    let right_rows = bounded_right_keys(&right_columns, right.num_rows(), MAX_DISTINCT_SET_KEYS)?;
    let mut emitted = HashSet::new();
    let mut selected = Vec::new();
    for row in 0..left.num_rows() {
        let key = BorrowedSetRow { columns: &left_columns, row };
        if !right_rows.contains(&key) && emitted.insert(key) {
            selected.push(row);
        }
    }
    left.take(&selected)?.relabel(schema)
}

fn right_counts<'a>(
    columns: &'a [&'a [Value]], rows: usize, max_distinct_keys: usize,
) -> Result<HashMap<BorrowedSetRow<'a>, usize>> {
    let mut counts = HashMap::<BorrowedSetRow<'a>, usize>::new();
    let mut row = 0usize;
    while row < rows {
        let key = BorrowedSetRow { columns, row };
        // Count a consecutive run with borrowed comparisons before hashing.
        let mut end = row + 1;
        while end < rows && key == (BorrowedSetRow { columns, row: end }) {
            end += 1;
        }
        let multiplicity = end - row;
        let at_limit = counts.len() >= max_distinct_keys;
        match counts.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let next = entry.get().checked_add(multiplicity)
                    .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
                *entry.get_mut() = next;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if at_limit {
                    return Err(Error::Execution("multiset distinct-key budget exceeded".into()));
                }
                entry.insert(multiplicity);
            }
        }
        row = end;
    }
    Ok(counts)
}

/// Keep each left row up to its multiplicity in the right input.
pub fn intersect_all(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.num_rows() == 0 || right.num_rows() == 0 {
        return Ok(Batch::empty(compatible_schema(left, right)?));
    }
    let (left, right, schema) = align_set_inputs(left, right)?;
    if schema.is_empty() {
        return Batch::new_with_row_count(schema, vec![], left.num_rows().min(right.num_rows()));
    }
    if schema.len() == 1 {
        let mut counts = HashMap::<&Value, usize>::new();
        for value in right.column(0)? {
            if counts.len() >= MAX_DISTINCT_SET_KEYS && !counts.contains_key(value) {
                return Err(Error::Execution("multiset distinct-key budget exceeded".into()));
            }
            let count = counts.entry(value).or_insert(0);
            *count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
        }
        let mut selected = Vec::new();
        for (index, value) in left.column(0)?.iter().enumerate() {
            if let Some(remaining) = counts.get_mut(value) {
                if *remaining > 0 { *remaining -= 1; selected.push(index); }
            }
        }
        return left.take(&selected)?.relabel(schema);
    }
    let left_columns = borrowed_columns(&left)?;
    let right_columns = borrowed_columns(&right)?;
    let mut counts = right_counts(&right_columns, right.num_rows(), MAX_DISTINCT_SET_KEYS)?;
    let mut selected = Vec::new();
    for row in 0..left.num_rows() {
        let key = BorrowedSetRow { columns: &left_columns, row };
        if let Some(remaining) = counts.get_mut(&key) {
            if *remaining > 0 {
                *remaining -= 1;
                selected.push(row);
            }
        }
    }
    left.take(&selected)?.relabel(schema)
}

/// Keep left occurrences after subtracting right row multiplicities.
pub fn except_all(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.num_rows() == 0 {
        return Ok(Batch::empty(compatible_schema(left, right)?));
    }
    let (left, right, schema) = align_set_inputs(left, right)?;
    if schema.is_empty() {
        return Batch::new_with_row_count(schema, vec![], left.num_rows().saturating_sub(right.num_rows()));
    }
    if schema.len() == 1 {
        let mut counts = HashMap::<&Value, usize>::new();
        for value in right.column(0)? {
            if counts.len() >= MAX_DISTINCT_SET_KEYS && !counts.contains_key(value) {
                return Err(Error::Execution("multiset distinct-key budget exceeded".into()));
            }
            let count = counts.entry(value).or_insert(0);
            *count = count.checked_add(1)
                .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
        }
        let mut selected = Vec::new();
        for (index, value) in left.column(0)?.iter().enumerate() {
            if let Some(remaining) = counts.get_mut(value) {
                if *remaining > 0 { *remaining -= 1; continue; }
            }
            selected.push(index);
        }
        return left.take(&selected)?.relabel(schema);
    }
    let left_columns = borrowed_columns(&left)?;
    let right_columns = borrowed_columns(&right)?;
    let mut counts = right_counts(&right_columns, right.num_rows(), MAX_DISTINCT_SET_KEYS)?;
    let mut selected = Vec::new();
    for row in 0..left.num_rows() {
        let key = BorrowedSetRow { columns: &left_columns, row };
        if let Some(remaining) = counts.get_mut(&key) {
            if *remaining > 0 {
                *remaining -= 1;
                continue;
            }
        }
        selected.push(row);
    }
    left.take(&selected)?.relabel(schema)
}

#[cfg(test)]
mod multiset_operator_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn set_all_retains_correct_duplicate_and_null_counts() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2)], vec![Value::Null],
            vec![Value::Integer(2)], vec![Value::Integer(2)],
            vec![Value::Null], vec![Value::Integer(3)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(2)],
            vec![Value::Integer(9)],
        ]).unwrap();
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(2)]
        ]);
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(3)]
        ]);
    }
}

#[cfg(test)]
mod bounded_right_set_key_regression {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn distinct_right_key_budget_checks_new_keys_only() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]).unwrap();
        let columns = borrowed_columns(&right).unwrap();
        assert!(bounded_right_keys(&columns, right.num_rows(), 1).is_err());
        assert_eq!(bounded_right_keys(&columns, right.num_rows(), 2).unwrap().len(), 2);
        let left = Batch::from_rows(schema, vec![vec![Value::Integer(2)]]).unwrap();
        assert_eq!(intersect_distinct(&left, &right).unwrap().row(0).unwrap(),
            vec![Value::Integer(2)]);
    }
}

#[cfg(test)]
mod except_bounded_right_set_regression {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn except_keeps_order_and_ignores_duplicate_right_keys() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(2)],
            vec![Value::Integer(3)], vec![Value::Integer(1)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(2)], vec![Value::Integer(2)],
        ]).unwrap();
        assert_eq!(except_distinct(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(1)],
        ]);
    }
}

#[cfg(test)]
mod streaming_set_row_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn large_duplicate_inputs_keep_stable_set_and_multiset_results() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), (0..200).map(|n| vec![Value::Integer(n % 4)]).collect()).unwrap();
        let right = Batch::from_rows(schema, (0..100).map(|n| vec![Value::Integer(n % 2)]).collect()).unwrap();
        assert_eq!(intersect_distinct(&left, &right).unwrap().column(0).unwrap(),
            &[Value::Integer(0), Value::Integer(1)]);
        assert_eq!(except_distinct(&left, &right).unwrap().column(0).unwrap(),
            &[Value::Integer(2), Value::Integer(3)]);
        assert_eq!(intersect_all(&left, &right).unwrap().num_rows(), 100);
        assert_eq!(except_all(&left, &right).unwrap().num_rows(), 100);
    }
}

#[cfg(test)]
mod bounded_multiset_state_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn duplicates_do_not_consume_additional_distinct_key_budget() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Null], vec![Value::Null],
            vec![Value::Integer(2)],
        ]).unwrap();
        let columns = borrowed_columns(&right).unwrap();
        assert!(right_counts(&columns, right.num_rows(), 2).is_err());
        let counts = right_counts(&columns, right.num_rows(), 3).unwrap();
        assert_eq!(counts.len(), 3);
        let null_key = BorrowedSetRow { columns: &columns, row: 2 };
        assert_eq!(counts[&null_key], 2);
        let left = Batch::from_rows(schema, vec![
            vec![Value::Null], vec![Value::Integer(1)], vec![Value::Integer(1)],
        ]).unwrap();
        assert_eq!(intersect_all(&left, &right).unwrap().num_rows(), 3);
        assert_eq!(except_all(&left, &right).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod native_single_column_set_operator_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn borrowed_keys_preserve_distinct_and_null_semantics() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
            vec![Value::Integer(1)], vec![Value::Integer(3)], vec![Value::Null]
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(2)]
        ]).unwrap();
        assert_eq!(intersect_distinct(&left, &right).unwrap().rows().unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(2)]]);
        assert_eq!(except_distinct(&left, &right).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    }
}

#[cfg(test)]
mod native_one_column_multiset_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn counts_duplicate_and_null_occurrences_in_input_order() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(3)], vec![Value::Null],
            vec![Value::Integer(3)], vec![Value::Integer(3)],
            vec![Value::Null], vec![Value::Integer(8)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(3)], vec![Value::Null],
            vec![Value::Integer(3)], vec![Value::Integer(3)],
            vec![Value::Integer(3)]
        ]).unwrap();
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(3)], vec![Value::Null],
            vec![Value::Integer(3)], vec![Value::Integer(3)]
        ]);
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(8)]
        ]);
    }
}

#[cfg(test)]
mod typed_empty_set_short_circuits_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn empty_inputs_keep_widened_result_schema_without_scanning_rows() {
        let integers = Batch::empty(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap());
        let doubles = Batch::from_rows(Schema::new(vec![
            Field::new("other", DataType::Float, true),
        ]).unwrap(), vec![vec![Value::Float(10.0)]]).unwrap();
        for result in [
            intersect_distinct(&integers, &doubles).unwrap(),
            intersect_all(&doubles, &integers).unwrap(),
            except_distinct(&integers, &doubles).unwrap(),
            except_all(&integers, &doubles).unwrap(),
        ] {
            assert_eq!(result.num_rows(), 0);
            assert_eq!(result.schema().fields()[0].data_type, DataType::Float);
            assert!(result.schema().fields()[0].nullable);
        }
        let invalid = Batch::empty(Schema::new(vec![
            Field::new("id", DataType::Text, false),
        ]).unwrap());
        assert!(intersect_distinct(&integers, &invalid).is_err());
    }
}

#[cfg(test)]
mod borrowed_multicolumn_set_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    fn inputs() -> (Batch, Batch) {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("score", DataType::Float, true),
        ]).unwrap();
        let long = "é🙂".repeat(2048);
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(long.clone()), Value::Float(-0.0)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Text(long.clone()), Value::Float(0.0)],
            vec![Value::Text("different".into()), Value::Null],
            vec![Value::Null, Value::Float(f64::NAN)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Text(long), Value::Float(0.0)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Null, Value::Float(f64::NAN)],
            vec![Value::Text("absent".into()), Value::Null],
        ]).unwrap();
        (left, right)
    }

    #[test]
    fn distinct_sets_preserve_first_rows_and_sql_null_float_equality() {
        let (left, right) = inputs();
        assert_eq!(intersect_distinct(&left, &right).unwrap().rows().unwrap(),
            vec![left.row(0).unwrap(), left.row(1).unwrap()]);
        assert_eq!(except_distinct(&left, &right).unwrap().rows().unwrap(),
            vec![left.row(3).unwrap()]);
    }

    #[test]
    fn multiset_counts_consume_right_duplicates_in_left_order() {
        let (left, right) = inputs();
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(),
            vec![left.row(0).unwrap(), left.row(1).unwrap(), left.row(4).unwrap()]);
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(),
            vec![left.row(2).unwrap(), left.row(3).unwrap()]);
    }

    #[test]
    fn borrowed_keys_enforce_cardinality_for_unique_not_duplicate_rows() {
        let (left, _) = inputs();
        let columns = borrowed_columns(&left).unwrap();
        assert!(bounded_right_keys(&columns, 2, 1).is_err());
        assert!(bounded_right_keys(&columns, 3, 2).is_ok());
        assert!(right_counts(&columns, 2, 1).is_err());
        assert!(right_counts(&columns, 3, 2).is_ok());
    }

    #[test]
    fn numeric_widening_and_empty_inputs_preserve_set_contracts() {
        let (left, _) = inputs();
        let empty = Batch::empty(left.schema().clone());
        assert!(intersect_all(&left, &empty).unwrap().rows().unwrap().is_empty());
        assert_eq!(except_all(&left, &empty).unwrap(), left);
        let a = Batch::from_rows(Schema::new(vec![
            Field::new("name", DataType::Text, false),
            Field::new("n", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Text("a".into()), Value::Integer(2)],
        ]).unwrap();
        let b = Batch::from_rows(Schema::new(vec![
            Field::new("tag", DataType::Text, false),
            Field::new("n", DataType::Float, false),
        ]).unwrap(), vec![
            vec![Value::Text("a".into()), Value::Float(2.0)],
        ]).unwrap();
        assert_eq!(intersect_distinct(&a, &b).unwrap().rows().unwrap(),
            vec![vec![Value::Text("a".into()), Value::Float(2.0)]]);
    }
}


#[cfg(test)]
mod numeric_set_alignment_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn mixed_integer_float_sets_share_aligned_keys_and_preserve_nulls() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("tag", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text("one".into())],
            vec![Value::Integer(2), Value::Text("two".into())],
            vec![Value::Integer(2), Value::Text("two".into())],
            vec![Value::Null, Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("other", DataType::Float, true),
            Field::new("tag", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Float(2.0), Value::Text("two".into())],
            vec![Value::Float(1.0), Value::Text("one".into())],
            vec![Value::Null, Value::Null],
        ]).unwrap();
        let (aligned_left, aligned_right, schema) = align_set_inputs(&left, &right).unwrap();
        assert!(matches!(&aligned_left, Cow::Owned(_)));
        assert!(matches!(&aligned_right, Cow::Borrowed(_)));
        assert_eq!(schema.fields()[0].data_type, DataType::Float);
        assert_eq!(aligned_left.value(0, 0).unwrap(), &Value::Float(1.0));
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Float(1.0), Value::Text("one".into())],
            vec![Value::Float(2.0), Value::Text("two".into())],
            vec![Value::Null, Value::Null],
        ]);
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Float(2.0), Value::Text("two".into())],
        ]);
        assert_eq!(except_distinct(&left, &right).unwrap().num_rows(), 0);
    }

    #[test]
    fn nullable_widening_retains_all_rows_for_large_numeric_inputs() {
        let left = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Integer, false),
        ]).unwrap(), (0..1024).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let right = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Float, true),
        ]).unwrap(), (0..1024).map(|n| vec![Value::Float(n as f64)]).collect()).unwrap();
        let (a, b, schema) = align_set_inputs(&left, &right).unwrap();
        assert_eq!(a.num_rows(), 1024);
        assert_eq!(b.num_rows(), 1024);
        assert_eq!(schema.fields()[0].data_type, DataType::Float);
        assert_eq!(intersect_distinct(&left, &right).unwrap().num_rows(), 1024);
    }
}

#[cfg(test)]
mod empty_key_set_multiplicity_regressions {
    use super::*;
    use crate::schema::Schema;

    #[test]
    fn zero_width_rows_are_one_key_with_distinct_or_all_multiplicity() {
        let schema = Schema::new(vec![]).unwrap();
        let mk = |rows| Batch::new_with_row_count(schema.clone(), vec![], rows).unwrap();
        let left = mk(5);
        let right = mk(3);
        assert_eq!(intersect_distinct(&left, &right).unwrap().num_rows(), 1);
        assert_eq!(except_distinct(&left, &right).unwrap().num_rows(), 0);
        assert_eq!(intersect_all(&left, &right).unwrap().num_rows(), 3);
        assert_eq!(except_all(&left, &right).unwrap().num_rows(), 2);
        assert_eq!(intersect_all(&mk(1), &mk(10)).unwrap().num_rows(), 1);
        assert_eq!(except_distinct(&left, &mk(0)).unwrap().num_rows(), 1);
        assert_eq!(except_all(&left, &mk(0)).unwrap().num_rows(), 5);
        assert_eq!(intersect_distinct(&left, &mk(0)).unwrap().num_rows(), 0);
        for output in [
            intersect_distinct(&left, &right).unwrap(),
            except_distinct(&left, &right).unwrap(),
            intersect_all(&left, &right).unwrap(),
            except_all(&left, &right).unwrap(),
        ] {
            assert!(output.schema().is_empty());
        }
    }
}

#[cfg(test)]
mod adaptive_set_membership_regressions {
    use super::*;

    #[test]
    fn smaller_left_side_preserves_nulls_ties_and_order() {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Float, true),
            Field::new("tag", DataType::Text, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Float(f64::NAN), Value::Text("match".into())],
            vec![Value::Float(-0.0), Value::Text("zero".into())],
            vec![Value::Null, Value::Text("missing".into())],
            vec![Value::Float(f64::NAN), Value::Text("match".into())],
        ]).unwrap();
        let mut right_rows = (0..500).map(|i| vec![
            Value::Float(i as f64 + 20.0), Value::Text("other".into()),
        ]).collect::<Vec<_>>();
        right_rows.push(vec![Value::Float(f64::NAN), Value::Text("match".into())]);
        right_rows.push(vec![Value::Float(0.0), Value::Text("zero".into())]);
        let right = Batch::from_rows(schema, right_rows).unwrap();
        assert_eq!(intersect_distinct(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Float(f64::NAN), Value::Text("match".into())],
            vec![Value::Float(-0.0), Value::Text("zero".into())],
        ]);
        assert_eq!(except_distinct(&left, &right).unwrap().rows().unwrap(), vec![
            vec![Value::Null, Value::Text("missing".into())],
        ]);
    }

    #[test]
    fn adaptive_one_column_sets_match_right_small_reference() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(),
            (0..20).map(|v| vec![Value::Integer(v % 7)]).collect()).unwrap();
        let right = Batch::from_rows(schema.clone(),
            (0..100).map(|v| vec![Value::Integer(v % 3)]).collect()).unwrap();
        let reference_right = Batch::from_rows(schema, vec![
            vec![Value::Integer(0)], vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        assert_eq!(intersect_distinct(&left, &right).unwrap(),
            intersect_distinct(&left, &reference_right).unwrap());
        assert_eq!(except_distinct(&left, &right).unwrap(),
            except_distinct(&left, &reference_right).unwrap());
    }
}

#[cfg(test)]
mod clustered_multiset_run_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn multi_column_runs_preserve_duplicate_counts_and_first_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("name", DataType::Text, true),
        ]).unwrap();
        let a = vec![Value::Integer(1), Value::Text("é".repeat(250))];
        let b = vec![Value::Null, Value::Null];
        let c = vec![Value::Integer(2), Value::Text("other".into())];
        let unseen = vec![Value::Integer(9), Value::Text("absent".into())];
        let left_rows = [vec![a.clone(); 5], vec![b.clone(); 2], vec![c.clone()]].concat();
        let right_rows = [
            vec![a.clone(); 3], vec![b.clone()],
            vec![unseen; 400],
        ].concat();
        let left = Batch::from_rows(schema.clone(), left_rows).unwrap();
        let right = Batch::from_rows(schema, right_rows).unwrap();
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(),
            [vec![a.clone(); 3], vec![b.clone()]].concat());
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(),
            [vec![a; 2], vec![b], vec![c]].concat());
    }
}
