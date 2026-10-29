use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("IF", args, 3, 3)?;
    if !matches!(args[0], Expr::Literal(Value::Null))
        && args[0].output_type(schema)? != DataType::Boolean {
        return Err(Error::Type("IF requires a boolean condition".into()));
    }
    unified_type(&args[1..], schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let matched = args[0].evaluate(batch, row)?.as_boolean()? == Some(true);
    args[if matched { 1 } else { 2 }].evaluate(batch, row)
}
