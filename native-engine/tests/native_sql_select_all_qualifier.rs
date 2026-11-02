use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn explicit_all_preserves_default_selection() {
    assert_eq!(rows("SELECT ALL 3 AS n"), vec![vec![Value::Integer(3)]]);
    assert_eq!(rows("SELECT ALL 1 + 2 AS n"), rows("SELECT 1 + 2 AS n"));
    assert!(parse_sql("SELECT ALL DISTINCT 1").is_err());
}
