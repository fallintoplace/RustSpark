use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn unary_plus_preserves_numeric_values_but_checks_types() {
    assert_eq!(rows("SELECT +3, +(-2), +NULL"),
        vec![vec![Value::Integer(3), Value::Integer(-2), Value::Null]]);
    assert_eq!(rows("SELECT +col1 FROM VALUES (5), (-2)"),
        vec![vec![Value::Integer(5)], vec![Value::Integer(-2)]]);
    assert_eq!(rows("SELECT col1 FROM VALUES (+5)"), vec![vec![Value::Integer(5)]]);
    for query in ["SELECT +'text'", "SELECT +TRUE"] {
        let plan = parse_sql(query).unwrap();
        assert!(execute(&plan, &Catalog::new()).is_err());
    }
}
