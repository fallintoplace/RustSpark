use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("ROUND", args, 1, 2)?;
    numeric_arg(&args[0], schema)?;
    if args.len() == 2 && !matches!(args[1], Expr::Literal(Value::Null))
        && args[1].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("ROUND precision must be an integer".into()));
    }
    Ok(DataType::Float)
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let number = match args[0].evaluate(batch, row)? {
        Value::Null => return Ok(Value::Null),
        Value::Integer(n) => n as f64,
        Value::Float(n) => n,
        _ => return Err(Error::Type("ROUND requires numeric input".into())),
    };
    let precision = if args.len() == 2 {
        match args[1].evaluate(batch, row)? {
            Value::Null => return Ok(Value::Null),
            Value::Integer(n) if (-12..=12).contains(&n) => n as i32,
            _ => return Err(Error::Type("ROUND precision must be within -12..12".into())),
        }
    } else { 0 };
    let factor = 10f64.powi(precision);
    let result = (number * factor).round() / factor;
    if !result.is_finite() {
        return Err(Error::Execution("ROUND non-finite result".into()));
    }
    Ok(Value::Float(result))
}
