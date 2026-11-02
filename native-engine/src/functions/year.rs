use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("YEAR", args, 1, 1)?;
    if !matches!(&args[0], Expr::Literal(Value::Null))
        && args[0].output_type(schema)? != DataType::Text
    {
        return Err(Error::Type("YEAR requires ISO date text".into()));
    }
    Ok(DataType::Integer)
}

/// Extract the Gregorian year from a validated YYYY-MM-DD date string.
/// This is an ISO text bridge until the native engine has a DATE scalar type.
pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Text(date) => {
            let bytes = date.as_bytes();
            if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-'
                || bytes.iter().enumerate().any(|(i, byte)|
                    i != 4 && i != 7 && !byte.is_ascii_digit())
            {
                return Err(Error::Type("YEAR requires YYYY-MM-DD date text".into()));
            }
            let year: i64 = date[0..4].parse()
                .map_err(|_| Error::Type("invalid YEAR input".into()))?;
            let month: usize = date[5..7].parse()
                .map_err(|_| Error::Type("invalid YEAR input".into()))?;
            let day: usize = date[8..10].parse()
                .map_err(|_| Error::Type("invalid YEAR input".into()))?;
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            let maximum = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 if leap => 29,
                2 => 28,
                _ => 0,
            };
            if year == 0 || day == 0 || day > maximum {
                return Err(Error::Type("invalid Gregorian date".into()));
            }
            Ok(Value::Integer(year))
        }
        _ => Err(Error::Type("YEAR requires ISO date text".into())),
    }
}
