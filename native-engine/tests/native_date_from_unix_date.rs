use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn from_unix_date_converts_signed_epoch_days() {
    assert_eq!(rows("SELECT DATE_FROM_UNIX_DATE(0)"), vec![vec![Value::Text("1970-01-01".into())]]);
    assert_eq!(rows("SELECT DATE_FROM_UNIX_DATE(1)"), vec![vec![Value::Text("1970-01-02".into())]]);
    assert_eq!(rows("SELECT DATE_FROM_UNIX_DATE(-1)"), vec![vec![Value::Text("1969-12-31".into())]]);
    assert_eq!(rows("SELECT DATE_FROM_UNIX_DATE(NULL)"), vec![vec![Value::Null]]);
    for q in ["SELECT DATE_FROM_UNIX_DATE(9223372036854775807)", "SELECT DATE_FROM_UNIX_DATE('0')"] {
        assert!(execute(&parse_sql(q).unwrap(), &Catalog::new()).is_err());
    }
}
