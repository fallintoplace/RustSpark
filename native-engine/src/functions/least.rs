use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("LEAST", args, 2, 64)?;
    unified_type(args, schema)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let mut least: Option<Value> = None;
    for arg in args {
        let candidate = arg.evaluate(batch, row)?;
        if candidate.is_null() { continue; }
        match &mut least {
            Some(value) => {
                let comparison = candidate.sql_compare(value)?
                    .ok_or_else(|| Error::Type("LEAST cannot order NaN".into()))?;
                if comparison == std::cmp::Ordering::Less {
                    *value = candidate;
                }
            }
            None => least = Some(candidate),
        }
    }
    Ok(least.unwrap_or(Value::Null))
}
