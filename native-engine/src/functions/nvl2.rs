use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NVL2", args, 3, 3)?;
    let left = &args[1];
    let right = &args[2];
    if matches!(left, Expr::Literal(Value::Null)) {
        return right.output_type(schema);
    }
    if matches!(right, Expr::Literal(Value::Null)) {
        return left.output_type(schema);
    }
    let a = left.output_type(schema)?;
    let b = right.output_type(schema)?;
    match (a, b) {
        (left, right) if left == right => Ok(left),
        (DataType::Integer, DataType::Float)
        | (DataType::Float, DataType::Integer) => Ok(DataType::Float),
        _ => Err(Error::Type("NVL2 result branches require compatible SQL types".into())),
    }
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let first = args[0].evaluate(batch, row)?;
    let result = args[if first.is_null() { 2 } else { 1 }].evaluate(batch, row)?;
    Ok(match (output_type(args, batch.schema())?, result) {
        (DataType::Float, Value::Integer(n)) => Value::Float(n as f64),
        (_, result) => result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn nvl2_widens_integer_and_float_results_without_losing_null_behavior() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(5)], vec![Value::Null],
        ]).unwrap();
        let args = vec![
            Expr::Column("x".into()),
            Expr::Literal(Value::Integer(10)),
            Expr::Literal(Value::Float(1.5)),
        ];
        assert_eq!(output_type(&args, batch.schema()).unwrap(), DataType::Float);
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(10.0));
        assert_eq!(eval(&args, &batch, 1).unwrap(), Value::Float(1.5));
    }
}
