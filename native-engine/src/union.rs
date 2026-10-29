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
    let mut columns = Vec::with_capacity(schema.len());
    for index in 0..schema.len() {
        let mut values = Vec::with_capacity(count);
        values.extend_from_slice(left.column(index)?);
        values.extend_from_slice(right.column(index)?);
        columns.push(values);
    }
    Batch::new(schema, columns)
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

 
/// UNION DISTINCT matches columns by position and removes repeated complete rows.
pub fn union_distinct(left: &Batch, right: &Batch) -> Result<Batch> {
    crate::distinct::distinct(&union_all(left, right)?)
}

#[cfg(test)]
mod union_distinct_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn distinct_set_union_collapses_null_and_duplicate_rows() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null],
        ]).unwrap();
        let b = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]).unwrap();
        assert_eq!(union_distinct(&a, &b).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)],
        ]);
    }
}


#[cfg(test)]
mod union_columnar_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn concatenation_keeps_row_order_and_nullability() {
        let lhs = Batch::from_rows(Schema::new(vec![
            Field::new("x", DataType::Integer, false),
            Field::new("label", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text("a".into())],
        ]).unwrap();
        let rhs = Batch::from_rows(Schema::new(vec![
            Field::new("y", DataType::Integer, true),
            Field::new("label", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Null, Value::Text("b".into())],
        ]).unwrap();
        let result = union_all(&lhs, &rhs).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.row(1).unwrap()[0], Value::Null);
        assert!(result.schema().field(0).unwrap().nullable);
        assert_eq!(result.schema().field(0).unwrap().name, "x");
    }
}
