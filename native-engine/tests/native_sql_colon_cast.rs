use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn double_colon_casts_preserve_sql_operator_precedence() {
    assert_eq!(run("SELECT '12'::INT").unwrap(), Value::Integer(12));
    assert_eq!(run("SELECT '12'::INT + 5").unwrap(), Value::Integer(17));
    assert_eq!(run("SELECT '12'::INT * 2").unwrap(), Value::Integer(24));
    assert_eq!(run("SELECT '12'::INT::STRING").unwrap(), Value::Text("12".into()));
    assert_eq!(run("SELECT (2 + 3)::DOUBLE").unwrap(), Value::Float(5.0));
    assert_eq!(run("SELECT NULL::BOOLEAN").unwrap(), Value::Null);
}

#[test]
fn double_colon_cast_rejects_unknown_types_and_invalid_values() {
    assert!(run("SELECT 'bad'::INT").is_err());
    assert!(parse_sql("SELECT 5::BLOB").is_err());
    assert!(parse_sql("SELECT 5:INT").is_err());
    assert!(parse_sql("SELECT 5::").is_err());
}
