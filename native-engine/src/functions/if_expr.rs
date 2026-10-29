use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("IF", args, 3, 3)?;
    if !matches!(args[0], Expr::Literal(Value::Null))
        && args[0].output_type(schema)? != DataType::Boolean {
        return Err(Error::Type("IF requires a boolean condition".into()));
    }
    let (then_expr, else_expr) = (&args[1], &args[2]);
    if matches!(then_expr, Expr::Literal(Value::Null)) {
        return else_expr.output_type(schema);
    }
    if matches!(else_expr, Expr::Literal(Value::Null)) {
        return then_expr.output_type(schema);
    }
    let a = then_expr.output_type(schema)?;
    let b = else_expr.output_type(schema)?;
    match (a, b) {
        (left, right) if left == right => Ok(left),
        (DataType::Integer, DataType::Float)
        | (DataType::Float, DataType::Integer) => Ok(DataType::Float),
        _ => Err(Error::Type("IF branches require compatible SQL types".into())),
    }
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let matched = args[0].evaluate(batch, row)?.as_boolean()? == Some(true);
    let value = args[if matched { 1 } else { 2 }].evaluate(batch, row)?;
    match value {
        Value::Integer(n) if output_type(args, batch.schema())? == DataType::Float =>
            Ok(Value::Float(n as f64)),
        value => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_if_branches_use_one_common_result_type() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![]]).unwrap();
        let yes = vec![
            Expr::Literal(Value::Boolean(true)),
            Expr::Literal(Value::Integer(42)),
            Expr::Literal(Value::Float(0.5)),
        ];
        assert_eq!(output_type(&yes, batch.schema()).unwrap(), DataType::Float);
        assert_eq!(eval(&yes, &batch, 0).unwrap(), Value::Float(42.0));
        let mut no = yes.clone();
        no[0] = Expr::Literal(Value::Boolean(false));
        assert_eq!(eval(&no, &batch, 0).unwrap(), Value::Float(0.5));
    }
}

#[cfg(test)]
mod if_noninteger_result_fast_path_tests {
    use super::*;
    #[test]
    fn text_null_and_floats_do_not_need_numeric_promotion() {
        let batch = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 1).unwrap();
        let text = [Expr::Literal(Value::Boolean(true)),
                    Expr::Literal(Value::Text("Berlin".into())),
                    Expr::Literal(Value::Text("Paris".into()))];
        assert_eq!(eval(&text, &batch, 0).unwrap(), Value::Text("Berlin".into()));
        let null = [Expr::Literal(Value::Boolean(true)),
                    Expr::Literal(Value::Null),
                    Expr::Literal(Value::Float(1.0))];
        assert_eq!(eval(&null, &batch, 0).unwrap(), Value::Null);
        let mixed = [Expr::Literal(Value::Boolean(true)),
                     Expr::Literal(Value::Integer(2)),
                     Expr::Literal(Value::Float(1.5))];
        assert_eq!(eval(&mixed, &batch, 0).unwrap(), Value::Float(2.0));
    }
}
