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
        Ok(Cow::Owned(Batch::new_with_row_count(
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
    for row in 0..rows {
        let key = BorrowedSetRow { columns, row };
        let at_limit = counts.len() >= max_distinct_keys;
        match counts.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let next = entry.get().checked_add(1)
                    .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
                *entry.get_mut() = next;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if at_limit {
                    return Err(Error::Execution("multiset distinct-key budget exceeded".into()));
                }
                entry.insert(1);
            }
        }
    }
    Ok(counts)
}

/// Keep each left row up to its multiplicity in the right input.
pub fn intersect_all(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.num_rows() == 0 || right.num_rows() == 0 {
        return Ok(Batch::empty(compatible_schema(left, right)?));
    }
    let (left, right, schema) = align_set_inputs(left, right)?;
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
