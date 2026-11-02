use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn substr_accepts_ansi_from_for_and_keeps_function_calls() {
    assert_eq!(rows("SELECT SUBSTR('Spark SQL' FROM 5 FOR 3)"),
        vec![vec![Value::Text("k S".into())]]);
    assert_eq!(rows("SELECT SUBSTR('Spark SQL' FROM -3)"),
        vec![vec![Value::Text("SQL".into())]]);
    assert_eq!(rows("SELECT SUBSTR('café' FROM 3 FOR 2)"),
        vec![vec![Value::Text("fé".into())]]);
    assert_eq!(rows("SELECT SUBSTR('Spark SQL', 5, 3)"),
        vec![vec![Value::Text("k S".into())]]);
    assert_eq!(rows("SELECT SUBSTR(NULL FROM 1 FOR 2)"), vec![vec![Value::Null]]);
    assert!(parse_sql("SELECT SUBSTR('abc' FROM 2 FOR)").is_err());
}
