use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn explicit_all_matches_default_aggregate_null_semantics() {
    assert_eq!(rows("SELECT COUNT(ALL col1), SUM(ALL col1), AVG(ALL col1) FROM VALUES (2), (NULL), (4)"), vec![
        vec![Value::Integer(2), Value::Integer(6), Value::Float(3.0)],
    ]);
    assert_eq!(rows("SELECT COUNT(DISTINCT col1) FROM VALUES (2), (2), (4)"),
        vec![vec![Value::Integer(2)]]);
}
#[test]
fn aggregate_all_rejects_distinct_following_it() {
    assert!(parse_sql("SELECT COUNT(ALL DISTINCT col1) FROM VALUES (1)").is_err());
}
