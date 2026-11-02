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
    let date = CalendarDate { year, month, day };
    if date.year == 0 || date.day == 0
        || date.day > month_length(date.year, date.month)
    {
        return Err(invalid());
    }
    Ok(Some(date))
}

fn date_integer_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("calendar function requires date text".into())) }
}

fn quarter(args: &[Value]) -> Result<Value> {
    let Some(date) = parse_iso_date(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Integer((date.month - 1) / 3 + 1))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "quarter", min_args: 1, max_args: 1,
        result_type: date_integer_type, call: quarter },
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
