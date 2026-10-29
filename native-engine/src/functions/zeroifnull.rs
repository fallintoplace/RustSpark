use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("ZEROIFNULL", args, 1, 1)?;
    numeric_arg(&args[0], schema)?;
    if matches!(args[0], Expr::Literal(Value::Null)) { return Ok(DataType::Integer); }
    args[0].output_type(schema)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => {
            let kind = output_type(args, batch.schema())?;
            Ok(if kind == DataType::Float { Value::Float(0.0) } else { Value::Integer(0) })
        },
        value @ (Value::Integer(_) | Value::Float(_)) => Ok(value),
        _ => Err(Error::Type("ZEROIFNULL requires numeric value".into())),
    }
}
