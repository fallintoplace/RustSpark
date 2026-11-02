use rustspark_native::{catalog::Catalog, executor, scalar::Value, sql::parse_sql};

fn run(query: &str) -> Vec<Vec<Value>> {
    executor::execute(&parse_sql(query).unwrap(), &Catalog::new())
        .unwrap().rows().unwrap()
}

#[test]
fn sorts_by_all_projected_columns_in_select_list_order() {
    let source = "FROM VALUES (2, 'b'), (1, 'a'), (2, 'a')";
    assert_eq!(run(&format!("SELECT col1 AS id, col2 AS tag {source} ORDER BY ALL")), vec![
        vec![Value::Integer(1), Value::Text("a".into())],
        vec![Value::Integer(2), Value::Text("a".into())],
        vec![Value::Integer(2), Value::Text("b".into())],
    ]);
    assert_eq!(run(&format!("SELECT col1 AS id, col2 AS tag {source} ORDER BY ALL DESC")), vec![
        vec![Value::Integer(2), Value::Text("b".into())],
        vec![Value::Integer(2), Value::Text("a".into())],
        vec![Value::Integer(1), Value::Text("a".into())],
    ]);
}

#[test]
fn all_resolves_expressions_aliases_and_distinct_output() {
    assert_eq!(run("SELECT col1 + 1 AS next FROM VALUES (2), (1) ORDER BY ALL"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
    assert_eq!(run("SELECT DISTINCT col1 AS id FROM VALUES (2), (1), (2) ORDER BY ALL"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    assert!(parse_sql("SELECT * FROM t ORDER BY ALL").is_err());
}
