use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn is_unknown_matches_nullable_boolean_semantics() {
    assert_eq!(run("SELECT NULL IS UNKNOWN").unwrap(), Value::Boolean(true));
    assert_eq!(run("SELECT TRUE IS UNKNOWN").unwrap(), Value::Boolean(false));
    assert_eq!(run("SELECT FALSE IS NOT UNKNOWN").unwrap(), Value::Boolean(true));
    assert_eq!(run("SELECT NULL IS NOT UNKNOWN").unwrap(), Value::Boolean(false));
    assert_eq!(run("SELECT (1 = NULL) IS UNKNOWN").unwrap(), Value::Boolean(true));
}

#[test]
fn unknown_rejects_non_boolean_values_but_is_null_does_not() {
    assert!(run("SELECT 42 IS UNKNOWN").is_err());
    assert!(run("SELECT 'a' IS NOT UNKNOWN").is_err());
    assert_eq!(run("SELECT 42 IS NULL").unwrap(), Value::Boolean(false));
    assert_eq!(run("SELECT 42 IS NOT NULL").unwrap(), Value::Boolean(true));
}
