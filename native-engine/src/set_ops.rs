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
