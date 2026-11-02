use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn value(sql: &str) -> Value {
    execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()[0][0].clone()
}

#[test]
fn date_add_and_sub_cover_tpch_day_offsets() {
    assert_eq!(value("SELECT DATE_SUB('1998-12-01', 90)"),
        Value::Text("1998-09-02".into()));
    assert_eq!(value("SELECT DATE_ADD('1994-01-01', 365)"),
        Value::Text("1995-01-01".into()));
    assert_eq!(value("SELECT DATE_ADD('2000-02-28', 1)"),
        Value::Text("2000-02-29".into()));
    assert_eq!(value("SELECT DATE_ADD('1999-12-31', 1)"),
        Value::Text("2000-01-01".into()));
    assert_eq!(value("SELECT DATE_ADD('2000-01-01', -1)"),
        Value::Text("1999-12-31".into()));
    assert_eq!(value("SELECT DATE_SUB(NULL, 7)"), Value::Null);
}

#[test]
fn invalid_dates_and_offsets_error() {
    for sql in [
        "SELECT DATE_ADD('1900-02-29', 1)",
        "SELECT DATE_ADD('1995-13-01', 1)",
        "SELECT DATE_SUB('0001-01-01', 1)",
        "SELECT DATE_ADD('9999-12-31', 1)",
        "SELECT DATE_SUB('2000-01-01', -9223372036854775808)",
        "SELECT DATE_ADD(1234, 1)",
    ] {
        assert!(execute(&parse_sql(sql).unwrap(), &Catalog::new()).is_err(), "{sql}");
    }
}
