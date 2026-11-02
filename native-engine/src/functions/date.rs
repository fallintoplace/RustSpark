//! ISO calendar date conversion for the native text-backed DATE bridge.
use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Schema}, Error, Result};

pub(crate) fn parse_iso_date(input: &str) -> Option<(i32, u32, u32)> {
    let bytes = input.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-'
        || !bytes.iter().enumerate().all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit()) {
        return None;
    }
    let year = input[..4].parse::<i32>().ok()?;
    let month = input[5..7].parse::<u32>().ok()?;
    let day = input[8..10].parse::<u32>().ok()?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if year < 1 || day < 1 || day > max_day { return None; }
    Some((year, month, day))
}

pub(crate) fn output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    if args.len() != 1 {
        return Err(Error::Parse("TO_DATE expects one argument".into()));
    }
    if args[0].output_type(schema)? != DataType::Text {
        return Err(Error::Type("TO_DATE requires ISO calendar text".into()));
    }
    Ok(DataType::Text)
}

pub(crate) fn eval(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    match args[0].evaluate(batch, row)? {
        Value::Null => Ok(Value::Null),
        Value::Text(value) if parse_iso_date(&value).is_some() =>
            Ok(Value::Text(value)),
        Value::Text(_) => Err(Error::Execution("invalid ISO calendar date".into())),
        _ => Err(Error::Type("TO_DATE requires ISO calendar text".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_leap_dates_and_rejects_invalid_calendar_days() {
        assert_eq!(parse_iso_date("1996-02-29"), Some((1996, 2, 29)));
        assert_eq!(parse_iso_date("2000-02-29"), Some((2000, 2, 29)));
        for invalid in ["1900-02-29", "1998-02-30", "1998-00-01", "1998-12-32",
            "0000-01-01", "1998-1-01", "1998-13-01"] {
            assert!(parse_iso_date(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn to_date_evaluates_in_native_sql() {
        let plan = crate::sql::parse_sql(
            "SELECT TO_DATE('1998-12-01') AS d, TO_DATE(NULL) AS missing"
        ).unwrap();
        let rows = crate::executor::execute(&plan, &crate::catalog::Catalog::new())
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("1998-12-01".into()), Value::Null]]);
    }
}
