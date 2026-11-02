use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

#[derive(Clone, Copy)]
struct CalendarDate {
    year: i64,
    month: i64,
    day: i64,
}

fn leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn month_length(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

// Dates are ISO text until the native engine has a physical DATE type.
fn parse_iso_date(value: &Value) -> Result<Option<CalendarDate>> {
    let Some(text) = super::string(value)? else { return Ok(None); };
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-'
        || !bytes.iter().enumerate().all(|(index, byte)|
            index == 4 || index == 7 || byte.is_ascii_digit())
    {
        return Err(Error::Type("calendar function requires YYYY-MM-DD text".into()));
    }
    let invalid = || Error::Type("invalid Gregorian calendar date".into());
    let year = text[..4].parse::<i64>().map_err(|_| invalid())?;
    let month = text[5..7].parse::<i64>().map_err(|_| invalid())?;
    let day = text[8..10].parse::<i64>().map_err(|_| invalid())?;
    if year == 0 || day == 0 || day > month_length(year, month) {
        return Err(invalid());
    }
    Ok(Some(CalendarDate { year, month, day }))
}

fn date_integer_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("calendar function requires date text".into())) }
}

fn quarter(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Integer((date.month - 1) / 3 + 1))
}

fn dayofyear(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    let mut ordinal = date.day;
    for month in 1..date.month {
        ordinal += month_length(date.year, month);
    }
    Ok(Value::Integer(ordinal))
}

// Whole Gregorian days since 1970-01-01, using integer calendar arithmetic.
fn days_since_epoch(date: CalendarDate) -> i64 {
    let year = if date.month <= 2 { date.year - 1 } else { date.year };
    let era = year.div_euclid(400);
    let year_in_era = year - era * 400;
    let month = date.month + if date.month > 2 { -3 } else { 9 };
    let day_in_year = (153 * month + 2) / 5 + date.day - 1;
    let day_in_era = year_in_era * 365 + year_in_era / 4
        - year_in_era / 100 + day_in_year;
    era * 146_097 + day_in_era - 719_468
}

fn dayofweek(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Integer((days_since_epoch(date) + 4).rem_euclid(7) + 1))
}

fn weekday(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Integer((days_since_epoch(date) + 3).rem_euclid(7)))
}

fn date_text_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("calendar function requires date text".into())) }
}

fn iso_text(date: CalendarDate) -> String {
    format!("{:04}-{:02}-{:02}", date.year, date.month, date.day)
}

fn last_day(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Text(iso_text(CalendarDate {
        day: month_length(date.year, date.month), ..date
    })))
}

fn dayname(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    const NAMES: &[&str; 7] = &[
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday",
    ];
    let index = (days_since_epoch(date) + 3).rem_euclid(7) as usize;
    Ok(Value::Text(NAMES[index].into()))
}

fn monthname(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    const MONTH_NAMES: &[&str; 12] = &[
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ];
    Ok(Value::Text(MONTH_NAMES[(date.month - 1) as usize].into()))
}

fn text_pair_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn named_weekday(text: &str) -> Option<i64> {
    const NAMES: &[(&str, i64)] = &[
        ("MO", 0), ("MON", 0), ("MONDAY", 0),
        ("TU", 1), ("TUE", 1), ("TUESDAY", 1),
        ("WE", 2), ("WED", 2), ("WEDNESDAY", 2),
        ("TH", 3), ("THU", 3), ("THURSDAY", 3),
        ("FR", 4), ("FRI", 4), ("FRIDAY", 4),
        ("SA", 5), ("SAT", 5), ("SATURDAY", 5),
        ("SU", 6), ("SUN", 6), ("SUNDAY", 6),
    ];
    NAMES.iter().find(|(name, _)| text.eq_ignore_ascii_case(name))
        .map(|(_, day)| *day)
}

fn next_day(args: &[Value]) -> Result<Value> {
    let Some(mut date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    let Some(day_name) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(target) = named_weekday(day_name) else { return Ok(Value::Null); };
    let today = (days_since_epoch(date) + 3).rem_euclid(7);
    let offset = (target - today).rem_euclid(7);
    // NEXT_DAY is strictly later than the supplied date.
    let offset = if offset == 0 { 7 } else { offset };
    for _ in 0..offset {
        date.day += 1;
        if date.day > month_length(date.year, date.month) {
            date.day = 1;
            date.month += 1;
            if date.month > 12 {
                date.month = 1;
                date.year += 1;
                if date.year > 9_999 {
                    return Err(Error::Execution("NEXT_DAY exceeds supported date range".into()));
                }
            }
        }
    }
    Ok(Value::Text(iso_text(date)))
}

fn trunc_date(args: &[Value]) -> Result<Value> {
    let Some(mut date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    let Some(format) = super::string(&args[1])? else { return Ok(Value::Null); };
    if ["YEAR", "YYYY", "YY"].iter().any(|name| format.eq_ignore_ascii_case(name)) {
        date.month = 1;
        date.day = 1;
    } else if ["MONTH", "MON", "MM"].iter().any(|name| format.eq_ignore_ascii_case(name)) {
        date.day = 1;
    } else {
        return Ok(Value::Null);
    }
    Ok(Value::Text(iso_text(date)))
}

fn iso_weeks_in_year(year: i64) -> i64 {
    let january_first = CalendarDate { year, month: 1, day: 1 };
    let weekday = (days_since_epoch(january_first) + 3).rem_euclid(7);
    if weekday == 3 || (weekday == 2 && leap_year(year)) { 53 } else { 52 }
}

fn weekofyear(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    let mut ordinal = date.day;
    for month in 1..date.month {
        ordinal += month_length(date.year, month);
    }
    let weekday = (days_since_epoch(date) + 3).rem_euclid(7);
    let mut week = (ordinal - weekday + 9).div_euclid(7);
    if week == 0 {
        week = iso_weeks_in_year(date.year - 1);
    } else if week > iso_weeks_in_year(date.year) {
        week = 1;
    }
    Ok(Value::Integer(week))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "quarter", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: quarter },
    Function { name: "dayofyear", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: dayofyear },
    Function { name: "dayofweek", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: dayofweek },
    Function { name: "weekday", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: weekday },
    Function { name: "last_day", min_args: 1, max_args: 1,
        result_type: date_text_type, call: last_day },
    Function { name: "dayname", min_args: 1, max_args: 1,
        result_type: date_text_type, call: dayname },
    Function { name: "monthname", min_args: 1, max_args: 1,
        result_type: date_text_type, call: monthname },
    Function { name: "next_day", min_args: 2, max_args: 2,
        result_type: text_pair_type, call: next_day },
    Function { name: "trunc", min_args: 2, max_args: 2,
        result_type: text_pair_type, call: trunc_date },
    Function { name: "weekofyear", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: weekofyear },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, schema::{Field, Schema}, sql};

    #[test]
    fn quarter_checks_calendar_boundaries_and_null() {
        assert_eq!(quarter(&[Value::Text("2024-01-01".into())]).unwrap(), Value::Integer(1));
        assert_eq!(quarter(&[Value::Text("2024-06-30".into())]).unwrap(), Value::Integer(2));
        assert_eq!(quarter(&[Value::Text("2024-07-01".into())]).unwrap(), Value::Integer(3));
        assert_eq!(quarter(&[Value::Text("2024-12-31".into())]).unwrap(), Value::Integer(4));
        assert_eq!(quarter(&[Value::Null]).unwrap(), Value::Null);
        assert!(quarter(&[Value::Text("2023-02-29".into())]).is_err());
        assert!(quarter(&[Value::Text("0000-01-01".into())]).is_err());
        assert!(quarter(&[Value::Text("2024-13-01".into())]).is_err());
        assert!(date_integer_type(&[DataType::Integer]).is_err());
    }

    #[test]
    fn quarter_runs_through_native_sql() {
        let schema = Schema::new(vec![Field::new("d", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Text("2024-02-29".into())],
            vec![Value::Text("2024-10-01".into())],
            vec![Value::Null],
        ]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("dates", batch).unwrap();
        let plan = sql::parse_sql("SELECT QUARTER(d) AS q FROM dates").unwrap();
        let rows = executor::execute(&plan, &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Integer(4)], vec![Value::Null],
        ]);
    }
}


#[cfg(test)]
mod dayofyear_tests {
    use super::*;
    #[test]
    fn handles_leap_years_and_date_validation() {
        assert_eq!(dayofyear(&[Value::Text("2024-02-29".into())]).unwrap(), Value::Integer(60));
        assert_eq!(dayofyear(&[Value::Text("2024-12-31".into())]).unwrap(), Value::Integer(366));
        assert_eq!(dayofyear(&[Value::Text("2023-12-31".into())]).unwrap(), Value::Integer(365));
        assert_eq!(dayofyear(&[Value::Null]).unwrap(), Value::Null);
        assert!(dayofyear(&[Value::Text("1900-02-29".into())]).is_err());
    }
}


#[cfg(test)]
mod dayofweek_tests {
    use super::*;
    #[test]
    fn sunday_is_one_and_thursday_is_five() {
        assert_eq!(days_since_epoch(CalendarDate { year: 1970, month: 1, day: 1 }), 0);
        assert_eq!(dayofweek(&[Value::Text("1970-01-01".into())]).unwrap(), Value::Integer(5));
        assert_eq!(dayofweek(&[Value::Text("2023-01-01".into())]).unwrap(), Value::Integer(1));
        assert_eq!(dayofweek(&[Value::Text("2024-01-01".into())]).unwrap(), Value::Integer(2));
        assert_eq!(dayofweek(&[Value::Null]).unwrap(), Value::Null);
    }
}


#[cfg(test)]
mod weekday_tests {
    use super::*;
    #[test]
    fn monday_is_zero_and_sunday_is_six() {
        assert_eq!(weekday(&[Value::Text("2024-01-01".into())]).unwrap(), Value::Integer(0));
        assert_eq!(weekday(&[Value::Text("2023-01-01".into())]).unwrap(), Value::Integer(6));
        assert_eq!(weekday(&[Value::Text("1970-01-01".into())]).unwrap(), Value::Integer(3));
        assert_eq!(weekday(&[Value::Null]).unwrap(), Value::Null);
    }
}


#[cfg(test)]
mod last_day_tests {
    use super::*;
    #[test]
    fn observes_gregorian_month_ends() {
        assert_eq!(last_day(&[Value::Text("2024-02-01".into())]).unwrap(), Value::Text("2024-02-29".into()));
        assert_eq!(last_day(&[Value::Text("1900-02-20".into())]).unwrap(), Value::Text("1900-02-28".into()));
        assert_eq!(last_day(&[Value::Text("2000-02-20".into())]).unwrap(), Value::Text("2000-02-29".into()));
        assert_eq!(last_day(&[Value::Text("9999-12-01".into())]).unwrap(), Value::Text("9999-12-31".into()));
        assert_eq!(last_day(&[Value::Null]).unwrap(), Value::Null);
        assert!(last_day(&[Value::Text("2024-04-31".into())]).is_err());
    }
}


#[cfg(test)]
mod dayname_tests {
    use super::*;
    #[test]
    fn identifies_weekdays_across_years() {
        assert_eq!(dayname(&[Value::Text("1970-01-01".into())]).unwrap(), Value::Text("Thursday".into()));
        assert_eq!(dayname(&[Value::Text("2024-02-29".into())]).unwrap(), Value::Text("Thursday".into()));
        assert_eq!(dayname(&[Value::Text("2023-01-01".into())]).unwrap(), Value::Text("Sunday".into()));
        assert_eq!(dayname(&[Value::Null]).unwrap(), Value::Null);
    }
}


#[cfg(test)]
mod monthname_tests {
    use super::*;
    #[test]
    fn names_months_and_preserves_nulls() {
        assert_eq!(monthname(&[Value::Text("2024-01-30".into())]).unwrap(), Value::Text("January".into()));
        assert_eq!(monthname(&[Value::Text("2024-02-29".into())]).unwrap(), Value::Text("February".into()));
        assert_eq!(monthname(&[Value::Text("2024-12-01".into())]).unwrap(), Value::Text("December".into()));
        assert_eq!(monthname(&[Value::Null]).unwrap(), Value::Null);
        assert!(monthname(&[Value::Text("2024-00-01".into())]).is_err());
    }
}


#[cfg(test)]
mod next_day_tests {
    use super::*;
    #[test]
    fn finds_strictly_later_weekdays_and_preserves_null() {
        assert_eq!(next_day(&[Value::Text("2024-01-01".into()), Value::Text("MONDAY".into())]).unwrap(),
            Value::Text("2024-01-08".into()));
        assert_eq!(next_day(&[Value::Text("2024-02-28".into()), Value::Text("Fri".into())]).unwrap(),
            Value::Text("2024-03-01".into()));
        assert_eq!(next_day(&[Value::Text("2024-12-29".into()), Value::Text("mo".into())]).unwrap(),
            Value::Text("2024-12-30".into()));
        assert_eq!(next_day(&[Value::Text("2024-01-01".into()), Value::Text("unknown".into())]).unwrap(),
            Value::Null);
        assert_eq!(next_day(&[Value::Null, Value::Text("FRI".into())]).unwrap(), Value::Null);
        assert_eq!(next_day(&[Value::Text("2024-01-01".into()), Value::Null]).unwrap(), Value::Null);
        assert!(next_day(&[Value::Text("9999-12-31".into()), Value::Text("SUN".into())]).is_err());
    }
}


#[cfg(test)]
mod trunc_date_tests {
    use super::*;
    #[test]
    fn truncates_calendar_years_and_months() {
        assert_eq!(trunc_date(&[Value::Text("2024-02-29".into()), Value::Text("YEAR".into())]).unwrap(),
            Value::Text("2024-01-01".into()));
        assert_eq!(trunc_date(&[Value::Text("2024-02-29".into()), Value::Text("mm".into())]).unwrap(),
            Value::Text("2024-02-01".into()));
        assert_eq!(trunc_date(&[Value::Text("2024-12-31".into()), Value::Text("mon".into())]).unwrap(),
            Value::Text("2024-12-01".into()));
        assert_eq!(trunc_date(&[Value::Text("2024-12-31".into()), Value::Text("DAY".into())]).unwrap(),
            Value::Null);
        assert_eq!(trunc_date(&[Value::Null, Value::Text("YEAR".into())]).unwrap(), Value::Null);
        assert_eq!(trunc_date(&[Value::Text("2024-01-01".into()), Value::Null]).unwrap(), Value::Null);
    }
}


#[cfg(test)]
mod weekofyear_tests {
    use super::*;
    #[test]
    fn uses_iso_weeks_and_handles_calendar_year_rollovers() {
        assert_eq!(weekofyear(&[Value::Text("2015-01-01".into())]).unwrap(), Value::Integer(1));
        assert_eq!(weekofyear(&[Value::Text("2016-01-01".into())]).unwrap(), Value::Integer(53));
        assert_eq!(weekofyear(&[Value::Text("2018-12-31".into())]).unwrap(), Value::Integer(1));
        assert_eq!(weekofyear(&[Value::Text("2020-12-31".into())]).unwrap(), Value::Integer(53));
        assert_eq!(weekofyear(&[Value::Text("2021-01-01".into())]).unwrap(), Value::Integer(53));
        assert_eq!(weekofyear(&[Value::Text("2024-12-30".into())]).unwrap(), Value::Integer(1));
        assert_eq!(weekofyear(&[Value::Null]).unwrap(), Value::Null);
    }
}
