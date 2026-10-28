use std::collections::HashSet;

use crate::{batch::Batch, Result};

/// Keep the first occurrence of each complete row. NULL values are equal for
/// DISTINCT/grouping, unlike WHERE's three-valued comparison semantics.
pub fn distinct(batch: &Batch) -> Result<Batch> {
    let mut seen = HashSet::new();
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        if seen.insert(batch.row(row)?) {
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
