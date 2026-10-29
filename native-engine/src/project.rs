use crate::{
    batch::Batch,
    expr::Expr,
    scalar::Value,
    schema::{Field, Schema},
    Result,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Selection {
    pub expression: Expr,
    pub alias: String,
}

impl Selection {
    pub fn new(expression: Expr, alias: impl Into<String>) -> Self {
        Self {
            expression,
            alias: alias.into(),
        }
    }
}

fn expression_nullable(expression: &Expr, schema: &Schema) -> Result<bool> {
    match expression {
        Expr::Column(name) => Ok(schema.field(schema.column(name)?)?.nullable),
        Expr::Literal(value) => Ok(value.is_null()),
        Expr::IsNull(_) | Expr::IsDistinctFrom { .. } => Ok(false),
        _ => Ok(true),
    }
}

pub fn project(batch: &Batch, selections: &[Selection]) -> Result<Batch> {
    let schema = Schema::new(
        selections
            .iter()
            .map(|selection| {
                Ok(Field::new(
                    &selection.alias,
                    selection.expression.output_type(batch.schema())?,
                    expression_nullable(&selection.expression, batch.schema())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )?;
    let mut columns: Vec<Vec<Value>> = selections
        .iter()
        .map(|_| Vec::with_capacity(batch.num_rows()))
        .collect();
    for (column, selection) in columns.iter_mut().zip(selections) {
        for row in 0..batch.num_rows() {
            column.push(selection.expression.evaluate(batch, row)?);
        }
    }
    Batch::new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::BinaryOp,
        scalar::Value,
        schema::{DataType, Field},
    };

    #[test]
    fn computed_columns_are_typed_and_aliased() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(4)]],
        )
        .unwrap();
        let result = project(
            &batch,
            &[Selection::new(
                Expr::binary(
                    Expr::Column("x".into()),
                    BinaryOp::Add,
                    Expr::Literal(Value::Integer(2)),
                ),
                "plus_two",
            )],
        )
        .unwrap();
        assert_eq!(result.schema().column("plus_two").unwrap(), 0);
        assert_eq!(result.row(0).unwrap(), vec![Value::Integer(6)]);
    }
}

 
/// Select existing column vectors without evaluating scalar expressions.
/// Unlike computed projection this preserves source field nullability.
pub fn project_columns(batch: &Batch, names: &[&str]) -> Result<Batch> {
    let mut fields = Vec::with_capacity(names.len());
    let mut values = Vec::with_capacity(names.len());
    for name in names {
        let position = batch.schema().column(name)?;
        fields.push(batch.schema().field(position)?.clone());
        values.push(batch.column(position)?.to_vec());
    }
    Batch::new(Schema::new(fields)?, values)
}

#[cfg(test)]
mod typed_projection_feature_tests {
    use super::*;
    use crate::{schema::{DataType, Field}, scalar::Value};

    #[test]
    fn keeps_schema_constraints_and_requested_order() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(3), Value::Null],
        ]).unwrap();
        let result = project_columns(&input, &["note", "id"]).unwrap();
        assert_eq!(result.row(0).unwrap(), vec![Value::Null, Value::Integer(3)]);
        assert!(result.schema().field(1).unwrap().nullable == false);
        assert!(project_columns(&input, &["id", "ID"]).is_err());
        assert!(project_columns(&input, &["unknown"]).is_err());
    }
}


#[cfg(test)]
mod selected_nullability_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn projects_required_column_and_nonnull_literal_as_not_nullable() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(42)]],
        ).unwrap();
        let result = project(&input, &[
            Selection::new(Expr::Column("id".into()), "key"),
            Selection::new(Expr::Literal(Value::Integer(1)), "one"),
            Selection::new(Expr::Literal(Value::Null), "missing"),
        ]).unwrap();
        assert!(!result.schema().field(0).unwrap().nullable);
        assert!(!result.schema().field(1).unwrap().nullable);
        assert!(result.schema().field(2).unwrap().nullable);
    }
}
