use rustspark_native::{
    catalog::Catalog, executor::execute, scalar::Value, sql::parse_sql,
};

#[test]
fn aliased_derived_columns_preserve_qualified_lookups() {
    let sql = "SELECT t.order_id, t.amount FROM (SELECT 7 AS id, 12 AS price) AS t(order_id, amount)";
    let rows = execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(7), Value::Integer(12)]]);
    let rows = execute(&parse_sql(
        "SELECT v.id FROM (VALUES (1), (2), (3)) AS v(id) WHERE v.id > 1 ORDER BY id"
    ).unwrap(), &Catalog::new()).unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn qualified_values_tables_can_join_without_colliding_names() {
    let sql = "SELECT l.id AS left_id, r.id AS right_id FROM (VALUES (1), (2)) l(id) \
               JOIN (VALUES (2), (3)) r(id) ON l.id = r.id";
    let rows = execute(&parse_sql(sql).unwrap(), &Catalog::new()).unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(2)]]);
}

#[test]
fn derived_alias_width_and_names_are_validated() {
    for sql in [
        "SELECT * FROM (SELECT 1 AS id) AS t(a, b)",
        "SELECT * FROM (SELECT 1 AS id, 2 AS value) t(a)",
        "SELECT * FROM (SELECT 1 AS id, 2 AS value) t(id, ID)",
    ] {
        let plan = parse_sql(sql).unwrap();
        assert!(execute(&plan, &Catalog::new()).is_err(), "{sql}");
    }
    assert!(parse_sql("SELECT * FROM (SELECT 1 AS value) t()").is_err());
}
