use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn fetch_first_row_only_defaults_to_one() {
    assert_eq!(rows("SELECT col1 FROM VALUES (3), (1), (2) ORDER BY col1 FETCH FIRST ROW ONLY"),
        vec![vec![Value::Integer(1)]]);
    assert_eq!(rows("SELECT col1 FROM VALUES (3), (1), (2) ORDER BY col1 OFFSET 1 ROWS FETCH NEXT ROW ONLY"),
        vec![vec![Value::Integer(2)]]);
}
#[test]
fn fetch_with_explicit_count_remains_supported() {
    assert_eq!(rows("SELECT col1 FROM VALUES (3), (1), (2) ORDER BY col1 FETCH FIRST 2 ROWS ONLY"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    assert!(parse_sql("SELECT 1 FETCH FIRST ONLY").is_err());
}
