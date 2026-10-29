use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("BIT_COUNT", args, 1, 1)?;
    if !matches!(args[0], Expr::Literal(Value::Null))
        && args[0].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("BIT_COUNT requires integer input".into()));
    }
    Ok(DataType::Integer)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(n.count_ones() as i64)),
        _ => Err(Error::Type("BIT_COUNT requires integer input".into())),
    }
}
