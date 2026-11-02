use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}
#[test]
fn cast_int64_and_float64_match_existing_integer_and_double_types() {
    assert_eq!(rows("SELECT CAST('7' AS INT64), CAST(2 AS FLOAT64), TRY_CAST('oops' AS INT64)"), vec![vec![
        Value::Integer(7), Value::Float(2.0), Value::Null,
    ]]);
    assert_eq!(rows("SELECT CAST(2 AS BIGINT), CAST(2 AS DOUBLE)"), vec![vec![
        Value::Integer(2), Value::Float(2.0),
    ]]);
}
#[test]
fn cast_aliases_do_not_hide_invalid_input() {
    let plan = parse_sql("SELECT CAST('bad' AS INT64)").unwrap();
    assert!(executor::execute(&plan, &Catalog::new()).is_err());
}
