use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("TRY_ADD", args, 2, 2)?;
    numeric_result_type(args, schema)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    try_numeric_binary(args, batch, row, i64::checked_add, |a, b| a + b)
}
