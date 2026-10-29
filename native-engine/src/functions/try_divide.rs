use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("TRY_DIVIDE", args, 2, 2)?;
    numeric_result_type(args, schema)?;
    Ok(DataType::Float)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    if left.is_null() || right.is_null() { return Ok(Value::Null); }
    let a = as_f64(&left)?;
    let b = as_f64(&right)?;
    if b == 0.0 { return Ok(Value::Null); }
    let result = a / b;
    if !result.is_finite() { return Ok(Value::Null); }
    Ok(Value::Float(result))
}
