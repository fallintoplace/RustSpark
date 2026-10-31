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
    for arg in args {
        let value = arg.evaluate(batch, row)?;
        if !value.is_null() {
            // The SQL planner has validated output types already. Only an
            // INTEGER result can need runtime widening to DOUBLE. Avoid
            // rescanning argument types for TEXT, DOUBLE and all-NULL rows.
            return Ok(match value {
                Value::Integer(n) if output_type(args, batch.schema())? == DataType::Float =>
                    Value::Float(n as f64),
                value => value,
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

#[cfg(test)]
mod coalesce_noninteger_fast_path_tests {
    use super::*;

    #[test]
    fn text_float_all_null_and_numeric_promotion_preserve_results() {
        let batch = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        let run = |values: &[Value]| {
            let args = values.iter().cloned().map(Expr::Literal).collect::<Vec<_>>();
            eval(&args, &batch, 0).unwrap()
        };
        assert_eq!(run(&[Value::Null, Value::Text("é🙂".into()),
            Value::Text("unused".into())]), Value::Text("é🙂".into()));
        assert_eq!(run(&[Value::Null, Value::Null]), Value::Null);
        assert_eq!(run(&[Value::Float(1.25), Value::Integer(2)]),
            Value::Float(1.25));
        assert_eq!(run(&[Value::Integer(2), Value::Null, Value::Float(0.5)]),
            Value::Float(2.0));
        assert!(output_type(&[Expr::Literal(Value::Text("x".into())),
            Expr::Literal(Value::Integer(1))], batch.schema()).is_err());
    }
}
