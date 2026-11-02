use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn values_can_be_used_without_select_or_from() {
    assert_eq!(rows("VALUES (1, 'one'), (2, 'two')"), vec![
        vec![Value::Integer(1), Value::Text("one".into())],
        vec![Value::Integer(2), Value::Text("two".into())],
    ]);
    assert_eq!(rows("VALUES (2), (1) ORDER BY col1 LIMIT 1"), vec![vec![Value::Integer(1)]]);
}
#[test]
fn standalone_values_reject_mismatched_row_widths() {
    assert!(parse_sql("VALUES (1), (2, 3)").is_err());
}
