use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

#[test]
fn spark_getbit_matches_bit_get_across_signed_values_and_nulls() {
    let catalog = Catalog::new();
    let result = execute(&parse_sql(
        "SELECT GETBIT(5, 0), GETBIT(5, 1), GETBIT(-1, 63), GETBIT(NULL, 2), BIT_GET(5, 2)"
    ).unwrap(), &catalog).unwrap();
    assert_eq!(result.row(0).unwrap(), vec![
        Value::Integer(1), Value::Integer(0), Value::Integer(1), Value::Null, Value::Integer(1),
    ]);
}

#[test]
fn spark_getbit_validates_integer_bit_positions() {
    let catalog = Catalog::new();
    assert!(execute(&parse_sql("SELECT GETBIT(5, 64)").unwrap(), &catalog).is_err());
    assert!(execute(&parse_sql("SELECT GETBIT('hello', 1)").unwrap(), &catalog).is_err());
}
