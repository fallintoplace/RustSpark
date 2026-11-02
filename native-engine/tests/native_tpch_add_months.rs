use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn value(sql: &str) -> Value {
    execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()[0][0].clone()
}

#[test]
fn tpch_month_and_year_windows_keep_calendar_boundaries() {
    assert_eq!(value("SELECT ADD_MONTHS('1993-07-01', 3)"),
        Value::Text("1993-10-01".into()));
    assert_eq!(value("SELECT ADD_MONTHS('1994-01-01', 12)"),
        Value::Text("1995-01-01".into()));
    assert_eq!(value("SELECT ADD_MONTHS('2020-01-31', 1)"),
        Value::Text("2020-02-29".into()));
    assert_eq!(value("SELECT ADD_MONTHS('2021-03-31', -1)"),
        Value::Text("2021-02-28".into()));
    assert_eq!(value("SELECT ADD_MONTHS(NULL, 3)"), Value::Null);
}

#[test]
fn invalid_month_offsets_and_calendar_inputs_fail() {
    for sql in [
        "SELECT ADD_MONTHS('0001-01-01', -1)",
        "SELECT ADD_MONTHS('9999-12-31', 1)",
        "SELECT ADD_MONTHS('2020-02-30', 1)",
        "SELECT ADD_MONTHS('2021-01-01', 9223372036854775807)",
        "SELECT ADD_MONTHS('2021-01-01', '3')",
    ] {
        assert!(execute(&parse_sql(sql).unwrap(), &Catalog::new()).is_err(), "{sql}");
    }
}
