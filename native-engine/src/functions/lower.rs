use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("LOWER", args, 1, 1)?;
    text_arg(&args[0], schema)?;
    Ok(DataType::Text)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => Ok(Value::Text(text.to_lowercase())),
        _ => Err(Error::Type("LOWER requires text".into())),
    }
}
