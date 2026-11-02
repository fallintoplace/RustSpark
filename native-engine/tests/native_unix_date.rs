use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn unix_date_counts_epoch_days() {
    assert_eq!(rows("SELECT UNIX_DATE(DATE '1970-01-01')"), vec![vec![Value::Integer(0)]]);
    assert_eq!(rows("SELECT UNIX_DATE('1969-12-31')"), vec![vec![Value::Integer(-1)]]);
    assert_eq!(rows("SELECT UNIX_DATE('1970-01-02')"), vec![vec![Value::Integer(1)]]);
    assert_eq!(rows("SELECT UNIX_DATE(NULL)"), vec![vec![Value::Null]]);
    for q in ["SELECT UNIX_DATE('2023-02-29')", "SELECT UNIX_DATE(7)"] {
        assert!(execute(&parse_sql(q).unwrap(), &Catalog::new()).is_err());
    }
}
