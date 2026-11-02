use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn value(query: &str) -> Value {
    execute(&parse_sql(query).unwrap(), &Catalog::new()).unwrap()
        .row(0).unwrap()[0].clone()
}

#[test]
fn long_is_spark_alias_of_bigint_for_cast() {
    assert_eq!(value("SELECT CAST('9223372036854775807' AS LONG)"), Value::Integer(i64::MAX));
    assert_eq!(value("SELECT TRY_CAST('7' AS LONG)"), Value::Integer(7));
    assert_eq!(value("SELECT CAST(NULL AS LONG)"), Value::Null);
    assert_eq!(value("SELECT TRY_CAST('not-a-number' AS LONG)"), Value::Null);
}

#[test]
fn long_still_enforces_integer_range() {
    let query = parse_sql("SELECT CAST('9223372036854775808' AS LONG)").unwrap();
    assert!(execute(&query, &Catalog::new()).is_err());
    assert_eq!(value("SELECT TRY_CAST('9223372036854775808' AS LONG)"), Value::Null);
}
