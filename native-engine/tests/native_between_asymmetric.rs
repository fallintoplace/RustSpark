use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};
fn rows(sql: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(sql).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn explicit_asymmetric_between_uses_normal_endpoint_order() {
    assert_eq!(rows("SELECT 2 BETWEEN ASYMMETRIC 1 AND 3, 2 BETWEEN ASYMMETRIC 3 AND 1, 2 NOT BETWEEN ASYMMETRIC 3 AND 1"), vec![vec![
        Value::Boolean(true), Value::Boolean(false), Value::Boolean(true),
    ]]);
}
#[test]
fn asymmetric_preserves_unknown_and_symmetric_still_works() {
    assert_eq!(rows("SELECT NULL BETWEEN ASYMMETRIC 1 AND 3, 2 BETWEEN SYMMETRIC 3 AND 1"), vec![vec![
        Value::Null, Value::Boolean(true),
    ]]);
}
