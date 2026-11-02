use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn result(query: &str) -> Value {
    execute(&parse_sql(query).unwrap(), &Catalog::new()).unwrap()
        .row(0).unwrap()[0].clone()
}

#[test]
fn overlay_accepts_ansi_placing_from_for() {
    assert_eq!(result("SELECT OVERLAY('Spark SQL' PLACING '_' FROM 6)"),
        Value::Text("Spark_SQL".into()));
    assert_eq!(result("SELECT OVERLAY('Spark SQL' PLACING 'CORE' FROM 7)"),
        Value::Text("Spark CORE".into()));
    assert_eq!(result("SELECT OVERLAY('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0)"),
        Value::Text("Spark ANSI SQL".into()));
    assert_eq!(result("SELECT OVERLAY('Spark SQL' PLACING 'tructured' FROM 2 FOR 4)"),
        Value::Text("Structured SQL".into()));
    assert_eq!(result("SELECT OVERLAY('aéZ' PLACING '🙂' FROM 2 FOR 1)"),
        Value::Text("a🙂Z".into()));
}

#[test]
fn overlay_still_accepts_function_args_and_rejects_malformed_sql() {
    assert_eq!(result("SELECT OVERLAY('abc', 'X', 2)"), Value::Text("aXc".into()));
    assert_eq!(result("SELECT OVERLAY(NULL PLACING 'X' FROM 2)"), Value::Null);
    assert!(parse_sql("SELECT OVERLAY('a' PLACING 'X' 2)").is_err());
    assert!(parse_sql("SELECT OVERLAY('a' PLACING FROM 2)").is_err());
}
