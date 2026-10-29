use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("COALESCE", args, 1, 64)?;
    unified_type(args, schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    for arg in args {
        let value = arg.evaluate(batch, row)?;
        if !value.is_null() {
            return Ok(value);
        }
    }
    Ok(Value::Null)
}
