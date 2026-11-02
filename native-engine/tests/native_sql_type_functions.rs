use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn run(query: &str) -> rustspark_native::Result<Value> {
    let plan = parse_sql(query)?;
    Ok(execute(&plan, &Catalog::new())?.row(0)?[0].clone())
}

#[test]
fn spark_scalar_conversion_functions_reuse_checked_casts() {
    assert_eq!(run("SELECT BIGINT('42')").unwrap(), Value::Integer(42));
    assert_eq!(run("SELECT DOUBLE('2.5')").unwrap(), Value::Float(2.5));
    assert_eq!(run("SELECT BOOLEAN('true')").unwrap(), Value::Boolean(true));
    assert_eq!(run("SELECT STRING(42)").unwrap(), Value::Text("42".into()));
    assert_eq!(run("SELECT STRING(NULL)").unwrap(), Value::Null);
    assert_eq!(run("SELECT BIGINT('7') + 3").unwrap(), Value::Integer(10));
}

#[test]
fn conversion_functions_reject_wrong_arity_and_invalid_values() {
    assert!(run("SELECT BIGINT('wrong')").is_err());
    assert!(run("SELECT DOUBLE('nan-text')").is_err());
    assert!(parse_sql("SELECT BOOLEAN()").is_err());
    assert!(parse_sql("SELECT STRING(1, 2)").is_err());
}
