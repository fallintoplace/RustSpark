use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("BIT_GET", args, 2, 2)?;
    for arg in args {
        if !matches!(arg, Expr::Literal(Value::Null))
            && arg.output_type(schema)? != DataType::Integer {
            return Err(Error::Type("BIT_GET requires integer inputs".into()));
        }
    }
    Ok(DataType::Integer)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let number = args[0].evaluate(batch, row)?;
    let position = args[1].evaluate(batch, row)?;
    match (number, position) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(value), Value::Integer(bit)) if (0..64).contains(&bit) =>
            Ok(Value::Integer(((value as u64 >> bit) & 1) as i64)),
        _ => Err(Error::Type("BIT_GET requires integer value and bit index 0..63".into())),
    }
}
