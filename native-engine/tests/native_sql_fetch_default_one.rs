use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn fetch_first_and_next_default_to_one_row() {
    assert_eq!(rows("SELECT 42 FETCH FIRST ROW ONLY"), vec![vec![Value::Integer(42)]]);
    assert_eq!(rows("SELECT 42 FETCH NEXT ROW ONLY"), vec![vec![Value::Integer(42)]]);
    assert_eq!(rows("SELECT 42 FETCH FIRST 0 ROWS ONLY"), Vec::<Vec<Value>>::new());
    assert!(parse_sql("SELECT 42 FETCH FIRST ONLY").is_err());
}
