use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("PMOD", args, 2, 2)?;
    numeric_result_type(args, schema)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    if left.is_null() || right.is_null() { return Ok(Value::Null); }
    if let (Value::Integer(a), Value::Integer(b)) = (&left, &right) {
        if *b == 0 { return Err(Error::Execution("PMOD division by zero".into())); }
        let dividend = i128::from(*a);
        let divisor = i128::from(*b);
        let result = if *b > 0 { dividend.rem_euclid(divisor) } else { dividend % divisor };
        return i64::try_from(result).map(Value::Integer)
            .map_err(|_| Error::Execution("PMOD integer overflow".into()));
    }
    let a = as_f64(&left)?;
    let b = as_f64(&right)?;
    if b == 0.0 { return Err(Error::Execution("PMOD division by zero".into())); }
    let value = if b > 0.0 { a.rem_euclid(b) } else { a % b };
    if !value.is_finite() { return Err(Error::Execution("PMOD non-finite result".into())); }
    Ok(Value::Float(value))
}

#[cfg(test)]
mod negative_divisor_sql_tests {
    use super::*;
    #[test]
    fn keeps_dividend_sign_when_divisor_is_negative() {
        let schema = Schema::new(vec![]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![]]).unwrap();
        for (left, right, expected) in [(-10, -3, -1), (10, -3, 1), (-10, 3, 2), (i64::MIN, -1, 0)] {
            let args = [Expr::Literal(Value::Integer(left)), Expr::Literal(Value::Integer(right))];
            assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Integer(expected));
        }
        let args = [Expr::Literal(Value::Float(-10.0)), Expr::Literal(Value::Float(-3.0))];
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(-1.0));
    }
}
