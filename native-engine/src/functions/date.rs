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

/// Shift a civil date without timezone or floating-point conversions.
pub(crate) fn shift_days(date: &str, amount: i64) -> Result<String> {
    let (year, month, day) = parse_iso_date(date)
        .ok_or_else(|| Error::Execution("invalid ISO calendar date".into()))?;
    if !(-4_000_000..=4_000_000).contains(&amount) {
        return Err(Error::Execution("date shift exceeds calendar range".into()));
    }
    let mut y = i64::from(year);
    let m = i64::from(month);
    let d = i64::from(day);
    if m <= 2 { y -= 1; }
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let ordinal = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    let shifted = ordinal + amount + 719468;
    let era = shifted.div_euclid(146097);
    let doe = shifted - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    if m <= 2 { y += 1; }
    if !(1..=9999).contains(&y) {
        return Err(Error::Execution("date shift exceeds calendar range".into()));
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

/// Shift months and clamp the day to the destination month's last day.
pub(crate) fn shift_months(date: &str, amount: i64) -> Result<String> {
    let (year, month, day) = parse_iso_date(date)
        .ok_or_else(|| Error::Execution("invalid ISO calendar date".into()))?;
    if !(-120_000..=120_000).contains(&amount) {
        return Err(Error::Execution("month shift exceeds calendar range".into()));
    }
    let absolute_month = i64::from(year) * 12 + i64::from(month) - 1 + amount;
    let new_year = absolute_month.div_euclid(12);
    let new_month = absolute_month.rem_euclid(12) + 1;
    if !(1..=9999).contains(&new_year) {
        return Err(Error::Execution("month shift exceeds calendar range".into()));
    }
    let leap = new_year % 4 == 0 && (new_year % 100 != 0 || new_year % 400 == 0);
    let max_day = match new_month {
        4 | 6 | 9 | 11 => 30_u32,
        2 if leap => 29,
        2 => 28,
        _ => 31,
    };
    Ok(format!("{new_year:04}-{new_month:02}-{:02}", day.min(max_day)))
}

pub(crate) fn add_months_output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    if args.len() != 2 {
        return Err(Error::Parse("ADD_MONTHS expects date and months".into()));
    }
    if args[0].output_type(schema)? != DataType::Text
        || args[1].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("ADD_MONTHS requires text date and integer months".into()));
    }
    Ok(DataType::Text)
}

pub(crate) fn eval_add_months(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let date = args[0].evaluate(batch, row)?;
    let months = args[1].evaluate(batch, row)?;
    match (date, months) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(date), Value::Integer(months)) =>
            Ok(Value::Text(shift_months(&date, months)?)),
        _ => Err(Error::Type("ADD_MONTHS requires text date and integer months".into())),
    }
}

pub(crate) fn shift_output_type(args: &[Expr], schema: &Schema) -> Result<DataType> {
    if args.len() != 2 {
        return Err(Error::Parse("DATE_ADD expects date and days".into()));
    }
    if args[0].output_type(schema)? != DataType::Text
        || args[1].output_type(schema)? != DataType::Integer {
        return Err(Error::Type("DATE_ADD requires text date and integer days".into()));
    }
    Ok(DataType::Text)
}

pub(crate) fn eval_add(args: &[Expr], batch: &Batch, row: usize) -> Result<Value> {
    let date = args[0].evaluate(batch, row)?;
    let days = args[1].evaluate(batch, row)?;
    match (date, days) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(date), Value::Integer(days)) =>
            Ok(Value::Text(shift_days(&date, days)?)),
        _ => Err(Error::Type("DATE_ADD requires text date and integer days".into())),
    }
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

#[cfg(test)]
mod date_add_tpch_tests {
    use super::*;

    #[test]
    fn shifts_across_leap_days_and_year_boundaries() {
        assert_eq!(shift_days("1996-02-28", 1).unwrap(), "1996-02-29");
        assert_eq!(shift_days("1996-03-01", -1).unwrap(), "1996-02-29");
        assert_eq!(shift_days("1998-12-31", 1).unwrap(), "1999-01-01");
        assert_eq!(shift_days("1970-01-01", 0).unwrap(), "1970-01-01");
        assert!(shift_days("0001-01-01", -1).is_err());
        assert!(shift_days("9999-12-31", 1).is_err());
    }

    #[test]
    fn native_date_add_preserves_nulls() {
        let plan = crate::sql::parse_sql(
            "SELECT DATE_ADD('1998-12-01', 90), DATE_ADD(NULL, 1)"
        ).unwrap();
        let rows = crate::executor::execute(&plan, &crate::catalog::Catalog::new())
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("1999-03-01".into()), Value::Null]]);
    }
}

#[cfg(test)]
mod tpch_add_months_tests {
    use super::*;

    #[test]
    fn month_end_shift_clamps_days_and_keeps_leap_years() {
        assert_eq!(shift_months("1996-01-31", 1).unwrap(), "1996-02-29");
        assert_eq!(shift_months("1997-01-31", 1).unwrap(), "1997-02-28");
        assert_eq!(shift_months("2000-02-29", 12).unwrap(), "2001-02-28");
        assert_eq!(shift_months("1998-01-15", -1).unwrap(), "1997-12-15");
        assert!(shift_months("0001-01-01", -1).is_err());
    }

    #[test]
    fn native_add_months_handles_nulls_and_month_boundaries() {
        let plan = crate::sql::parse_sql(
            "SELECT ADD_MONTHS('1998-11-30', 3), ADD_MONTHS(NULL, 1)"
        ).unwrap();
        let rows = crate::executor::execute(&plan, &crate::catalog::Catalog::new())
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("1999-02-28".into()), Value::Null]]);
    }
}
