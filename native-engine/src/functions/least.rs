use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    super::extrema::output_type("LEAST", args, schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    super::extrema::eval("LEAST", std::cmp::Ordering::Less, args, batch, row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Field;

    #[test]
    fn least_supports_mixed_numeric_types_and_null_values() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(2)], vec![Value::Integer(8)], vec![Value::Null],
        ]).unwrap();
        let args = vec![Expr::Column("x".into()), Expr::Literal(Value::Float(5.5))];
        assert_eq!(output_type(&args, batch.schema()).unwrap(), DataType::Float);
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(2.0));
        assert_eq!(eval(&args, &batch, 1).unwrap(), Value::Float(5.5));
        assert_eq!(eval(&args, &batch, 2).unwrap(), Value::Float(5.5));
    }
}
