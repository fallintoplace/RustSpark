use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("TRY_MULTIPLY", args, 2, 2)?;
    numeric_result_type(args, schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    if left.is_null() || right.is_null() { return Ok(Value::Null); }
    if let (Value::Integer(a), Value::Integer(b)) = (&left, &right) {
        return Ok(a.checked_mul(*b).map(Value::Integer).unwrap_or(Value::Null));
    }
    let a = as_f64(&left)?;
    let b = as_f64(&right)?;
    let result = a * b;
    Ok(if result.is_finite() { Value::Float(result) } else { Value::Null })
}
