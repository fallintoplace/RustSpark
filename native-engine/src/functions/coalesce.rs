use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("COALESCE", args, 1, 64)?;
    let mut output = None;
    for expression in args {
        if matches!(expression, Expr::Literal(Value::Null)) { continue; }
        let next = expression.output_type(schema)?;
        output = Some(match (output, next) {
            (None, kind) => kind,
            (Some(kind), other) if kind == other => kind,
            (Some(DataType::Integer), DataType::Float)
            | (Some(DataType::Float), DataType::Integer) => DataType::Float,
            _ => return Err(Error::Type("COALESCE arguments require compatible SQL types".into())),
        });
    }
    Ok(output.unwrap_or(DataType::Text))
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let result_type = output_type(args, batch.schema())?;
    for arg in args {
        let value = arg.evaluate(batch, row)?;
        if !value.is_null() {
            return Ok(match (result_type, value) {
                (DataType::Float, Value::Integer(n)) => Value::Float(n as f64),
                (_, value) => value,
            });
        }
    }
    Ok(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn widens_integer_values_to_double_without_evaluating_unused_fallback() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(5)], vec![Value::Null],
        ]).unwrap();
        let args = vec![
            Expr::Column("x".into()), Expr::Literal(Value::Float(2.5)),
        ];
        assert_eq!(output_type(&args, batch.schema()).unwrap(), DataType::Float);
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(5.0));
        assert_eq!(eval(&args, &batch, 1).unwrap(), Value::Float(2.5));
        assert!(output_type(&[
            Expr::Literal(Value::Text("x".into())),
            Expr::Literal(Value::Integer(1)),
        ], batch.schema()).is_err());
    }
}
