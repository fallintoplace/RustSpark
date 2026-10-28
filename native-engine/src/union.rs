use crate::{
    batch::Batch,
    schema::{Field, Schema},
    Error,
    Result,
};

pub fn union_all(left: &Batch, right: &Batch) -> Result<Batch> {
    if left.schema().len() != right.schema().len() {
        return Err(Error::Schema("UNION column counts must match".into()));
    }
    let mut fields = Vec::new();
    for (a, b) in left.schema().fields().iter().zip(right.schema().fields()) {
        if a.data_type != b.data_type {
            return Err(Error::Type(format!("UNION type mismatch in column {}", a.name)));
        }
        fields.push(Field::new(
            &a.name,
            a.data_type,
            a.nullable || b.nullable,
        ));
    }
    let schema = Schema::new(fields)?;
    let count = left
        .num_rows()
        .checked_add(right.num_rows())
        .ok_or_else(|| Error::Execution("UNION row count overflow".into()))?;
    let mut rows = Vec::with_capacity(count);
    rows.extend(left.rows()?);
    rows.extend(right.rows()?);
    Batch::from_rows(schema, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scalar::Value,
        schema::{DataType, Field},
    };

    #[test]
    fn union_preserves_duplicates_and_nullable_values() {
        let a = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        )
        .unwrap();
        let b = Batch::from_rows(
            Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(1)]],
        )
        .unwrap();
        let merged = union_all(&a, &b).unwrap();
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.row(1).unwrap(), vec![Value::Null]);
    }
}
