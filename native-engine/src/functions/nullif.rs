use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("NULLIF", args, 2, 2)?;
    let _ = args[1].output_type(schema)?;
    args[0].output_type(schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let first = args[0].evaluate(batch, row)?;
    if first.is_null() { return Ok(Value::Null); }
    let second = args[1].evaluate(batch, row)?;
    if first.sql_equal(&second)? == Some(true) {
        Ok(Value::Null)
    } else {
        Ok(first)
    }
}
