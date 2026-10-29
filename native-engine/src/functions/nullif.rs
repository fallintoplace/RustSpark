use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NULLIF", args, 2, 2)?;
    let first = args[0].output_type(schema)?;
    let second = args[1].output_type(schema)?;
    if !matches!(args[0], Expr::Literal(Value::Null))
        && !matches!(args[1], Expr::Literal(Value::Null))
        && first != second
        && !matches!((first, second),
            (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer)) {
        return Err(Error::Type("NULLIF requires comparable arguments".into()));
    }
    Ok(first)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let first = args[0].evaluate(batch, row)?;
    if first.is_null() { return Ok(Value::Null); }
    let second = args[1].evaluate(batch, row)?;
    if first.sql_equal(&second)? == Some(true) {
        Ok(Value::Null)
    } else {
        Ok(first)
    }
}

#[cfg(test)]
mod nullif_numeric_sql_tests {
    use super::*;
    #[test]
    fn accepts_cross_numeric_comparisons_but_rejects_incompatible_types() {
        let schema = Schema::new(vec![]).unwrap();
        let int_float = [Expr::Literal(Value::Integer(9_007_199_254_740_993)),
            Expr::Literal(Value::Float(9_007_199_254_740_992.0))];
        assert_eq!(output_type(&int_float, &schema).unwrap(), DataType::Integer);
        let batch = Batch::from_rows(schema.clone(), vec![vec![]]).unwrap();
        assert_eq!(eval(&int_float, &batch, 0).unwrap(), Value::Integer(9_007_199_254_740_993));
        let incompatible = [Expr::Literal(Value::Text("1".into())), Expr::Literal(Value::Integer(1))];
        assert!(output_type(&incompatible, &schema).is_err());
    }
}
