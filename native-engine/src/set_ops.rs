//! Native SQL set operators with explicit schema compatibility checks.
use std::collections::HashSet;
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

/// INTERSECT ALL consumes each matching right-side occurrence once.
pub fn intersect_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_schema(left, right)?;
    let mut counts = std::collections::HashMap::<Vec<crate::scalar::Value>, usize>::new();
    for row in right.rows()? {
        *counts.entry(row).or_default() += 1;
    }
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

/// EXCEPT ALL preserves each left occurrence not consumed by a right occurrence.
pub fn except_all(left: &Batch, right: &Batch) -> Result<Batch> {
    let schema = compatible_schema(left, right)?;
    let mut counts = std::collections::HashMap::<Vec<crate::scalar::Value>, usize>::new();
    for row in right.rows()? {
        *counts.entry(row).or_default() += 1;
    }
    let mut rows = Vec::new();
    for row in left.rows()? {
        match counts.get_mut(&row) {
            Some(remaining) if *remaining > 0 => *remaining -= 1,
            _ => rows.push(row),
        }
    }
    Batch::from_rows(schema, rows)
}

#[cfg(test)]
mod multiset_engine_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn duplicate_and_null_multiplicity_is_preserved() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Null],
        ]).unwrap();
        let right = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        assert_eq!(intersect_all(&left, &right).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Null]]);
        assert_eq!(except_all(&left, &right).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Null]]);
    }
}
