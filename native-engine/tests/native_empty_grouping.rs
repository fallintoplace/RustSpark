use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn empty_grouping_set_matches_global_aggregation() {
    assert_eq!(rows("SELECT COUNT(*), SUM(col1) FROM VALUES (1), (2), (3) GROUP BY ()"),
        vec![vec![Value::Integer(3), Value::Integer(6)]]);
    assert_eq!(rows("SELECT COUNT(*) FROM VALUES (1), (1) GROUP BY ()"),
        vec![vec![Value::Integer(2)]]);
}
#[test]
fn invalid_empty_grouping_tail_is_rejected() {
    assert!(parse_sql("SELECT COUNT(*) FROM VALUES (1) GROUP BY (,)").is_err());
}
