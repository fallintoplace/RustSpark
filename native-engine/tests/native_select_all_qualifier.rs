use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn select_all_keeps_duplicates_and_explicit_distinct_still_deduplicates() {
    assert_eq!(rows("SELECT ALL col1 FROM VALUES (1), (1), (2)"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Integer(2)],
    ]);
    assert_eq!(rows("SELECT DISTINCT col1 FROM VALUES (1), (1), (2) ORDER BY col1"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
    ]);
}
#[test]
fn select_all_rejects_conflicting_qualifiers() {
    assert!(parse_sql("SELECT ALL DISTINCT 1").is_err());
}
