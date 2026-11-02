use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}
#[test]
fn any_value_uses_first_group_value_and_optional_null_policy() {
    assert_eq!(rows("SELECT ANY_VALUE(col1) FROM VALUES (2), (3)"), vec![vec![Value::Integer(2)]]);
    assert_eq!(rows("SELECT ANY_VALUE(col1) FROM VALUES (NULL), (3)"), vec![vec![Value::Null]]);
    assert_eq!(rows("SELECT ANY_VALUE(col1, TRUE) FROM VALUES (NULL), (3)"), vec![vec![Value::Integer(3)]]);
    assert!(parse_sql("SELECT ANY_VALUE(col1, 1) FROM VALUES (1)").is_err());
}
#[test]
fn any_value_can_appear_in_having() {
    assert_eq!(rows("SELECT COUNT(*) FROM VALUES (2), (3) HAVING ANY_VALUE(col1) = 2"),
        vec![vec![Value::Integer(2)]]);
}
