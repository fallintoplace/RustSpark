use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn spark_inline_values_provides_typed_named_columns_without_catalog() {
    assert_eq!(rows(
        "SELECT id, city FROM VALUES (1, 'Berlin'), (2, 'Paris') AS t(id, city) ORDER BY id DESC"
    ), vec![
        vec![Value::Integer(2), Value::Text("Paris".into())],
        vec![Value::Integer(1), Value::Text("Berlin".into())],
    ]);
    assert_eq!(rows(
        "SELECT col1, col2 FROM VALUES (1, 'a'), (2, 'b')"
    ), vec![
        vec![Value::Integer(1), Value::Text("a".into())],
        vec![Value::Integer(2), Value::Text("b".into())],
    ]);
    assert_eq!(rows("SELECT * FROM VALUES (1 + 2, CAST('5' AS INT)) AS t(a, b)"),
        vec![vec![Value::Integer(3), Value::Integer(5)]]);
}

#[test]
fn spark_values_null_and_numeric_type_widening_are_preserved() {
    assert_eq!(rows("SELECT x FROM VALUES (1), (2.5), (NULL) AS t(x)"), vec![
        vec![Value::Float(1.0)], vec![Value::Float(2.5)], vec![Value::Null],
    ]);
    assert_eq!(rows(
        "SELECT grp, SUM(n) AS total FROM VALUES (1, 2), (1, 3), (2, NULL)          AS t(grp, n) GROUP BY grp ORDER BY grp"
    ), vec![
        vec![Value::Integer(1), Value::Integer(5)],
        vec![Value::Integer(2), Value::Null],
    ]);
}

#[test]
fn inline_values_aliases_and_row_shapes_fail_closed() {
    for sql in [
        "SELECT * FROM VALUES (1, 2), (3)",
        "SELECT * FROM VALUES (1, 'hi'), ('oops', 'other')",
        "SELECT * FROM VALUES (1, 2) AS t(x)",
        "SELECT * FROM VALUES (1, 2) AS t(x, x)",
        "SELECT * FROM VALUES (1, 2) AS t(x, y, z)",
        "SELECT * FROM VALUES (other)",
        "SELECT * FROM VALUES (1), (2, 3)",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
}
