use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn result(sql: &str) -> Value {
    execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()[0][0].clone()
}

#[test]
fn tpch_date_parts_extract_from_iso_calendar_values() {
    assert_eq!(result("SELECT EXTRACT(MONTH FROM '1995-03-15')"), Value::Integer(3));
    assert_eq!(result("SELECT EXTRACT(DAY FROM '2000-02-29')"), Value::Integer(29));
    assert_eq!(result("SELECT MONTH('1998-12-01')"), Value::Integer(12));
    assert_eq!(result("SELECT DAYOFMONTH('1998-12-01')"), Value::Integer(1));
    assert_eq!(result("SELECT EXTRACT(MONTH FROM NULL)"), Value::Null);
}

#[test]
fn unsupported_parts_and_invalid_calendars_fail() {
    assert!(parse_sql("SELECT EXTRACT(HOUR FROM '1995-03-15')").is_err());
    for sql in [
        "SELECT MONTH('1900-02-29')",
        "SELECT DAY('1998-02-30')",
        "SELECT DAY('1995-00-01')",
    ] {
        assert!(execute(&parse_sql(sql).unwrap(), &Catalog::new()).is_err(), "{sql}");
    }
}
