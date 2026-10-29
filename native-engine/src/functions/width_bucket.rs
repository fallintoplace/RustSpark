use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("WIDTH_BUCKET", args, 4, 4)?;
    for arg in &args[..3] { numeric_arg(arg, schema)?; }
    if !matches!(args[3], Expr::Literal(Value::Null))
        && args[3].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("WIDTH_BUCKET bucket count must be integer".into()));
    }
    Ok(DataType::Integer)
}
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let values = args.iter().map(|arg| arg.evaluate(batch, row))
        .collect::<Result<Vec<_>>>()?;
    if values.iter().any(Value::is_null) { return Ok(Value::Null); }
    let value = as_f64(&values[0])?;
    let lower = as_f64(&values[1])?;
    let upper = as_f64(&values[2])?;
    let buckets = match values[3] {
        Value::Integer(n) if (1..=1_000_000).contains(&n) => n,
        _ => return Err(Error::Type("WIDTH_BUCKET requires 1..=1000000 buckets".into())),
    };
    if !value.is_finite() || !lower.is_finite() || !upper.is_finite()
        || lower >= upper || !(upper - lower).is_finite() {
        return Err(Error::Execution("WIDTH_BUCKET requires finite ordered bounds".into()));
    }
    let bucket = if value < lower {
        0
    } else if value >= upper {
        buckets + 1
    } else {
        (((value - lower) / (upper - lower)) * buckets as f64).floor() as i64 + 1
    };
    Ok(Value::Integer(bucket))
}
