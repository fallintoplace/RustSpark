use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn asymmetric_between_is_ordered_and_null_aware() {
    assert_eq!(rows("SELECT 2 BETWEEN ASYMMETRIC 1 AND 3"), vec![vec![Value::Boolean(true)]]);
    assert_eq!(rows("SELECT 2 BETWEEN ASYMMETRIC 3 AND 1"), vec![vec![Value::Boolean(false)]]);
    assert_eq!(rows("SELECT NULL BETWEEN ASYMMETRIC 1 AND 3"), vec![vec![Value::Null]]);
    assert_eq!(rows("SELECT 2 NOT BETWEEN ASYMMETRIC 1 AND 3"), vec![vec![Value::Boolean(false)]]);
}
