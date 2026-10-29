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
        if let Expr::Column(name) = &selection.expression {
            let index = batch.schema().column(name)?;
            column.extend_from_slice(batch.column(index)?);
        } else if let Expr::Literal(value) = &selection.expression {
            column.resize(batch.num_rows(), value.clone());
        } else {
            for row in 0..batch.num_rows() {
                column.push(selection.expression.evaluate(batch, row)?);
            }
        }
    }
    Batch::new_with_row_count(schema, columns, batch.num_rows())
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
    Batch::new_with_row_count(Schema::new(fields)?, values, batch.num_rows())
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

#[cfg(test)]
mod zero_column_projection_consolidation_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn both_projection_paths_keep_zero_width_row_counts() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let source = Batch::from_rows(schema, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        for projected in [project(&source, &[]).unwrap(), project_columns(&source, &[]).unwrap()] {
            assert_eq!(projected.schema().len(), 0);
            assert_eq!(projected.rows().unwrap(), vec![vec![]; 2]);
        }
    }
}

#[cfg(test)]
mod direct_column_projection_perf_tests {
    use super::*;
    use crate::{expr::BinaryOp, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn column_fast_path_preserves_aliases_duplicates_and_computed_values() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(3)]],
        ).unwrap();
        let result = project(&input, &[
            Selection::new(Expr::Column("n".into()), "first"),
            Selection::new(Expr::Column("n".into()), "again"),
            Selection::new(Expr::binary(
                Expr::Column("n".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(2)),
            ), "computed"),
        ]).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Null, Value::Null, Value::Null],
            vec![Value::Integer(3), Value::Integer(3), Value::Integer(5)],
        ]);
        assert!(project(&input, &[Selection::new(
            Expr::Column("unknown".into()), "missing"
        )]).is_err());
    }
}

#[cfg(test)]
mod projection_fast_paths_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn direct_columns_and_literals_match_scalar_evaluation() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("a".into())], vec![Value::Null]],
        ).unwrap();
        let selections = [
            Selection::new(Expr::Column("name".into()), "first"),
            Selection::new(Expr::Column("NAME".into()), "again"),
            Selection::new(Expr::Literal(Value::Integer(7)), "constant"),
            Selection::new(Expr::Literal(Value::Null), "missing"),
        ];
        let projected = project(&input, &selections).unwrap();
        assert_eq!(projected.rows().unwrap(), vec![
            vec![Value::Text("a".into()), Value::Text("a".into()), Value::Integer(7), Value::Null],
            vec![Value::Null, Value::Null, Value::Integer(7), Value::Null],
        ]);
        assert_eq!(project(&Batch::empty(input.schema().clone()), &selections).unwrap().num_rows(), 0);
    }
}
