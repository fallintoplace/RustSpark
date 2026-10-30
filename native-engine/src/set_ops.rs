//! Native SQL set operators with explicit schema compatibility checks.
use std::{borrow::Cow, collections::{HashMap, HashSet}};
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

fn bounded_right_keys(right: &Batch, max_keys: usize) -> Result<HashSet<Vec<crate::scalar::Value>>> {
    let mut keys = HashSet::new();
    for row in 0..right.num_rows() {
        let value = right.row(row)?;
        if keys.len() >= max_keys && !keys.contains(&value) {
            return Err(Error::Execution("set operation distinct-key budget exceeded".into()));
        }
        keys.insert(value);
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
    let right_rows = bounded_right_keys(&right, MAX_DISTINCT_SET_KEYS)?;
    let mut emitted = HashSet::new();
    let mut rows = Vec::new();
    for index in 0..left.num_rows() {
        let row = left.row(index)?;
        if right_rows.contains(&row) && emitted.insert(row.clone()) {
            rows.push(row);
        }
    }
    Batch::from_rows(schema, rows)
}

pub fn except_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    except_distinct_with_max_rows(left, right, usize::MAX)
}

pub fn except_distinct_with_max_rows(
    left: &Batch, right: &Batch, max_output_rows: usize,
) -> Result<Batch> {
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
            if !right_values.contains(value) && emitted.insert(value) {
                if rows.len() >= max_output_rows {
                    return Err(Error::Execution("EXCEPT DISTINCT output row limit exceeded".into()));
                }
                rows.push(index);
            }
        }
        return left.take(&rows)?.relabel(schema);
    }
    let right_rows = bounded_right_keys(right, MAX_DISTINCT_SET_KEYS)?;
    let mut emitted = HashSet::new();
    let mut rows = Vec::new();
    for index in 0..left.num_rows() {
        let row = left.row(index)?;
        if !right_rows.contains(&row) && emitted.insert(row.clone()) {
            if rows.len() >= max_output_rows {
                return Err(Error::Execution("EXCEPT DISTINCT output row limit exceeded".into()));
            }
            rows.push(row);
        }
    }
    Batch::from_rows(schema, rows)
}

fn right_counts(
    right: &Batch, max_distinct_keys: usize,
) -> Result<HashMap<Vec<crate::scalar::Value>, usize>> {
    let mut counts = HashMap::<Vec<crate::scalar::Value>, usize>::new();
    for index in 0..right.num_rows() {
        let row = right.row(index)?;
        let at_limit = counts.len() >= max_distinct_keys;
        match counts.entry(row) {
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
    let mut counts = right_counts(&right, MAX_DISTINCT_SET_KEYS)?;
    let mut rows = Vec::new();
    for index in 0..left.num_rows() {
        let row = left.row(index)?;
        if let Some(remaining) = counts.get_mut(&row) {
            if *remaining > 0 {
                *remaining -= 1;
                rows.push(row);
            }
        }
    }
    Batch::from_rows(schema, rows)
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
    let mut counts = right_counts(right, MAX_DISTINCT_SET_KEYS)?;
    let mut rows = Vec::new();
    for index in 0..left.num_rows() {
        let row = left.row(index)?;
        if let Some(remaining) = counts.get_mut(&row) {
            if *remaining > 0 {
                *remaining -= 1;
                continue;
            }
        }
        rows.push(row);
    }
    Batch::from_rows(schema, rows)
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
        assert!(bounded_right_keys(&right, 1).is_err());
        assert_eq!(bounded_right_keys(&right, 2).unwrap().len(), 2);
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
        assert!(right_counts(&right, 2).is_err());
        let counts = right_counts(&right, 3).unwrap();
        assert_eq!(counts.len(), 3);
        assert_eq!(counts[&vec![Value::Null]], 2);
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
mod bounded_except_distinct_result_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn caps_distinct_output_before_cloning_many_result_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Integer(3)],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![vec![Value::Integer(1)]]).unwrap();
        assert!(except_distinct_with_max_rows(&left, &right, 1)
            .unwrap_err().to_string().contains("output row limit"));
        assert_eq!(except_distinct_with_max_rows(&left, &right, 2).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
    }
}
