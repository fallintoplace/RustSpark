use rustspark_native::{catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql};

#[test]
fn derived_relations_support_select_union_filter_sort_and_limit() {
    let catalog = Catalog::new();
    let query = "SELECT x.n FROM (SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n) AS x \
                 WHERE x.n > 1 ORDER BY n DESC LIMIT 2";
    assert_eq!(execute(&parse_sql(query).unwrap(), &catalog).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(3)], vec![Value::Integer(2)]
    ]);
    let nested = "SELECT outer_q.n FROM (SELECT inner_q.n AS n FROM (SELECT 7 AS n) inner_q) outer_q";
    assert_eq!(execute(&parse_sql(nested).unwrap(), &catalog).unwrap().row(0).unwrap(), vec![Value::Integer(7)]);
}

#[test]
fn derived_relations_can_join_with_equality_keys() {
    let query = "SELECT l.id AS id, r.label AS name FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) l \
                 LEFT JOIN (SELECT 2 AS id, 'matched' AS label) r ON l.id = r.id ORDER BY id";
    assert_eq!(execute(&parse_sql(query).unwrap(), &Catalog::new()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(1), Value::Null],
        vec![Value::Integer(2), Value::Text("matched".into())],
    ]);
}

#[test]
fn unsupported_sources_and_deep_subqueries_are_rejected() {
    for sql in [
        "SELECT * FROM (DELETE FROM users) bad",
        "SELECT * FROM (SELECT 1 AS value)",
        "SELECT * FROM (SELECT 1 AS value) t(other)",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
    let mut sql = String::from("SELECT 1 AS value");
    for _ in 0..65 {
        sql = format!("SELECT value FROM ({sql}) t");
    }
    assert!(parse_sql(&sql).is_err());
}
