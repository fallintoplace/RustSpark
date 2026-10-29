use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

fn query(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn values_support_rows_projection_aggregation_order_and_nulls() {
    assert_eq!(query("SELECT id FROM (VALUES (2), (1), (3)) AS v(id) ORDER BY id"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]
    ]);
    assert_eq!(query("SELECT id, SUM(amount) AS total FROM (VALUES (1, 2), (1, 3), (2, 4)) AS v(id, amount) GROUP BY id ORDER BY id"), vec![
        vec![Value::Integer(1), Value::Integer(5)], vec![Value::Integer(2), Value::Integer(4)]
    ]);
    assert_eq!(query("SELECT id FROM (VALUES (1), (NULL), (3)) AS t(id) WHERE id IS NULL"), vec![
        vec![Value::Null]
    ]);
    assert_eq!(query("SELECT n FROM (VALUES (1), (2.5)) AS t(n) ORDER BY n"), vec![
        vec![Value::Float(1.0)], vec![Value::Float(2.5)]
    ]);
    assert_eq!(query("SELECT col1 FROM (VALUES (7)) AS t"), vec![vec![Value::Integer(7)]]);
}

#[test]
fn values_reject_ragged_rows_invalid_types_and_alias_width() {
    for sql in [
        "SELECT * FROM (VALUES (1, 2), (3)) AS t(a, b)",
        "SELECT * FROM (VALUES (1), ('text')) AS t(a)",
        "SELECT * FROM (VALUES (1, 2)) AS t(a)",
        "SELECT * FROM (VALUES (1)) AS t(a, a)",
        "SELECT * FROM (VALUES (missing)) AS t(a)",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
}
