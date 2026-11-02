use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};
fn run(query: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    execute(&parse_sql(query)?, &Catalog::new())?.rows()
}
#[test]
fn explicit_all_keeps_rows_and_does_not_enable_distinct() {
    assert_eq!(run("SELECT ALL 7 AS n").unwrap(), vec![vec![Value::Integer(7)]]);
    assert_eq!(run("SELECT ALL 7 AS n UNION ALL SELECT ALL 7 AS n").unwrap(),
        vec![vec![Value::Integer(7)], vec![Value::Integer(7)]]);
    assert_eq!(run("SELECT ALL NULL AS n").unwrap(), vec![vec![Value::Null]]);
}
