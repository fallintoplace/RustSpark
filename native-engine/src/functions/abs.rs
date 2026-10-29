use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("ABS", args, 1, 1)?;
    numeric_arg(&args[0], schema)?;
    args[0].output_type(schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => n.checked_abs().map(Value::Integer)
            .ok_or_else(|| Error::Execution("ABS integer overflow".into())),
        Value::Float(n) if n.is_finite() => Ok(Value::Float(n.abs())),
        _ => Err(Error::Type("ABS requires numeric argument".into())),
    }
}
