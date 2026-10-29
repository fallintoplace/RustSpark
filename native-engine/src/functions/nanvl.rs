use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NANVL", args, 2, 2)?;
    numeric_result_type(args, schema)?;
    Ok(DataType::Float)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    if left.is_null() { return Ok(Value::Null); }
    let number = as_f64(&left)?;
    if !number.is_nan() { return Ok(Value::Float(number)); }
    let fallback = args[1].evaluate(batch, row)?;
    if fallback.is_null() { return Ok(Value::Null); }
    Ok(Value::Float(as_f64(&fallback)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nan_uses_fallback_without_datafusion() {
        let batch = Batch::empty(Schema::new(vec![]).unwrap());
        let args = [
            Expr::Literal(Value::Float(f64::NAN)),
            Expr::Literal(Value::Float(7.0)),
        ];
        assert_eq!(eval(&args, &batch, 0).unwrap(), Value::Float(7.0));
    }
}
