use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn value(sql: &str) -> Value {
    execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()[0][0].clone()
}

#[test]
fn tpch_month_and_year_intervals_respect_real_calendar_months() {
    assert_eq!(value("SELECT '1993-07-01' + INTERVAL '3' MONTH"),
        Value::Text("1993-10-01".into()));
    assert_eq!(value("SELECT '1994-01-01' + INTERVAL '1' YEAR"),
        Value::Text("1995-01-01".into()));
    assert_eq!(value("SELECT '2020-01-31' + INTERVAL '1' MONTH"),
        Value::Text("2020-02-29".into()));
    assert_eq!(value("SELECT '2020-02-29' - INTERVAL '1' YEAR"),
        Value::Text("2019-02-28".into()));
}

#[test]
fn invalid_and_overflowing_calendar_intervals_fail() {
    for sql in [
        "SELECT '1994-01-01' + INTERVAL '1.5' MONTH",
        "SELECT '1994-01-01' + INTERVAL '9223372036854775807' YEAR",
        "SELECT '0001-01-01' - INTERVAL '1' YEAR",
    ] {
        let result = parse_sql(sql);
        assert!(result.is_err() || execute(&result.unwrap(), &Catalog::new()).is_err(), "{sql}");
    }
}
