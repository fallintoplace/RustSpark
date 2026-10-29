use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NULLIFZERO", args, 1, 1)?;
    numeric_arg(&args[0], schema)?;
    args[0].output_type(schema)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null | Value::Integer(0) => Ok(Value::Null),
        Value::Float(n) if n == 0.0 => Ok(Value::Null),
        value @ (Value::Integer(_) | Value::Float(_)) => Ok(value),
        _ => Err(Error::Type("NULLIFZERO requires numeric value".into())),
    }
}
