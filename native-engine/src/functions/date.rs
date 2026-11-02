use super::*;

pub(super) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    check_arity("TO_DATE", args, 1, 1)?;
    if !matches!(&args[0], Expr::Literal(Value::Null))
        && args[0].output_type(schema)? != DataType::Text {
        return Err(Error::Type("DATE requires ISO text input".into()));
    }
    Ok(DataType::Text)
}

pub(super) fn validate_iso_date(text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-'
        || bytes.iter().enumerate().any(|(i, byte)|
            i != 4 && i != 7 && !byte.is_ascii_digit())
    {
        return Err(Error::Type("DATE requires YYYY-MM-DD".into()));
    }
    let year: u32 = text[0..4].parse()
        .map_err(|_| Error::Type("invalid DATE year".into()))?;
    let month: u32 = text[5..7].parse()
        .map_err(|_| Error::Type("invalid DATE month".into()))?;
    let day: u32 = text[8..10].parse()
        .map_err(|_| Error::Type("invalid DATE day".into()))?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > days {
        return Err(Error::Type("invalid Gregorian DATE".into()));
    }
    Ok(())
}

pub(super) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => {
            validate_iso_date(&text)?;
            Ok(Value::Text(text))
        }
        _ => Err(Error::Type("DATE requires ISO text input".into())),
    }
}
