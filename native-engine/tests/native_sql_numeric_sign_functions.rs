use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

#[test]
fn spark_sign_functions_preserve_numeric_types_and_nulls() {
    let catalog = Catalog::new();
    let result = execute(
        &parse_sql("SELECT POSITIVE(-3) AS p, NEGATIVE(5) AS n, NEGATIVE(2.5) AS f, NEGATIVE(NULL) AS z").unwrap(),
        &catalog,
    ).unwrap();
    assert_eq!(result.row(0).unwrap(), vec![
        Value::Integer(-3), Value::Integer(-5), Value::Float(-2.5), Value::Null,
    ]);
}

#[test]
fn spark_sign_functions_reject_nonnumeric_inputs() {
    let catalog = Catalog::new();
    assert!(execute(&parse_sql("SELECT NEGATIVE('hello')").unwrap(), &catalog).is_err());
    assert!(execute(&parse_sql("SELECT POSITIVE('hello')").unwrap(), &catalog).is_err());
}
