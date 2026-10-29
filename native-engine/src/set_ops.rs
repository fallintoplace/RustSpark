//! Native SQL set operators with explicit schema compatibility checks.
use std::collections::{HashMap, HashSet};
use crate::{batch::Batch, schema::{Field, Schema}, Error, Result};

pub fn compatible_schema(left: &Batch, right: &Batch) -> Result<Schema> {
    if left.schema().len() != right.schema().len() {
        return Err(Error::Schema("set operation column counts must match".into()));
    }
    let fields = left.schema().fields().iter().zip(right.schema().fields()).map(|(a, b)| {
        if a.data_type != b.data_type {
            return Err(Error::Type(format!("set operation type mismatch at {}", a.name)));
        }
        Ok(Field::new(&a.name, a.data_type, a.nullable || b.nullable))
    }).collect::<Result<Vec<_>>>()?;
    Schema::new(fields)
}

pub fn intersect_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_schema(left, right)?;
    let right_rows: HashSet<_> = right.rows()?.into_iter().collect();
    let mut emitted = HashSet::new();
    let mut rows = Vec::new();
    for row in left.rows()? {
        if right_rows.contains(&row) && emitted.insert(row.clone()) {
            rows.push(row);
        }
    }
    Batch::from_rows(schema, rows)
}

pub fn except_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_schema(left, right)?;
    let right_rows: HashSet<_> = right.rows()?.into_iter().collect();
    let mut emitted = HashSet::new();
    let mut rows = Vec::new();
    for row in left.rows()? {
        if !right_rows.contains(&row) && emitted.insert(row.clone()) {
            rows.push(row);
        }
    }
    Batch::from_rows(schema, rows)
}

fn right_counts(right: &Batch) -> Result<HashMap<Vec<crate::scalar::Value>, usize>> {
    let mut counts = HashMap::<Vec<crate::scalar::Value>, usize>::new();
    for row in right.rows()? {
        let entry = counts.entry(row).or_insert(0);
        *entry = entry.checked_add(1)
            .ok_or_else(|| Error::Execution("set row multiplicity overflow".into()))?;
    }
    Ok(counts)
}

/// Keep each left row up to its multiplicity in the right input.
pub fn intersect_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_schema(left, right)?;
    let mut counts = right_counts(right)?;
    let mut rows = Vec::new();
    for row in left.rows()? {
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
    let schema = compatible_schema(left, right)?;
    let mut counts = right_counts(right)?;
    let mut rows = Vec::new();
    for row in left.rows()? {
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
