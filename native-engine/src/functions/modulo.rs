use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("MOD", args, 2, 2)?;
    numeric_result_type(args, schema)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let left = args[0].evaluate(batch, row)?;
    let right = args[1].evaluate(batch, row)?;
    if left.is_null() || right.is_null() { return Ok(Value::Null); }
    if let (Value::Integer(a), Value::Integer(b)) = (&left, &right) {
        return a.checked_rem(*b).map(Value::Integer)
            .ok_or_else(|| Error::Execution("MOD invalid integer divisor".into()));
    }
    let a = as_f64(&left)?;
    let b = as_f64(&right)?;
    if b == 0.0 { return Err(Error::Execution("MOD division by zero".into())); }
    let result = a % b;
    if !result.is_finite() { return Err(Error::Execution("MOD non-finite result".into())); }
    Ok(Value::Float(result))
}
