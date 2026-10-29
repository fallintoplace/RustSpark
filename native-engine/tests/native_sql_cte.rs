use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(sql: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Text, false),
        Field::new("n", DataType::Integer, false),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(schema, vec![
        vec![Value::Text("Berlin".into()), Value::Integer(2)],
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Paris".into()), Value::Integer(5)],
    ]).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn spark_cte_expands_over_existing_tables_and_aggregates() {
    assert_eq!(run(
        "WITH filtered AS (SELECT city, n FROM sales WHERE n >= 3)          SELECT city, SUM(n) AS total FROM filtered GROUP BY city ORDER BY city"
    ), vec![
        vec![Value::Text("Berlin".into()), Value::Integer(3)],
        vec![Value::Text("Paris".into()), Value::Integer(5)],
    ]);
}

#[test]
fn spark_cte_chains_column_aliases_and_repeated_references() {
    assert_eq!(run(
        "WITH base(x, label) AS (SELECT 1 AS id, 'one' AS name),               next AS (SELECT x + 1 AS id FROM base)          SELECT id FROM next"
    ), vec![vec![Value::Integer(2)]]);
    assert_eq!(run(
        "WITH base AS (SELECT 2 AS v)          SELECT * FROM base UNION ALL SELECT * FROM base"
    ), vec![vec![Value::Integer(2)], vec![Value::Integer(2)]]);
    assert_eq!(run(
        "WITH base(x) AS (SELECT * FROM VALUES (3), (4) AS t(n))          SELECT x FROM base ORDER BY x"
    ), vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
}

#[test]
fn spark_cte_respects_inner_scope_over_outer_name() {
    assert_eq!(run(
        "WITH t AS (SELECT 1 AS value)          SELECT value FROM t"
    ), vec![vec![Value::Integer(1)]]);
}

#[test]
fn spark_cte_rejects_recursion_duplicates_and_alias_mismatches() {
    for query in [
        "WITH RECURSIVE t AS (SELECT 1 AS n) SELECT n FROM t",
        "WITH t AS (SELECT 1 AS n), T AS (SELECT 2 AS n) SELECT n FROM t",
        "WITH t(x, y) AS (SELECT 1 AS n) SELECT x FROM t",
        "WITH t AS (SELECT n FROM t) SELECT n FROM t",
        "WITH t AS (SELECT n FROM sales) SELECT n FROM t; SELECT 9 FROM sales",
    ] {
        assert!(parse_sql(query).is_err(), "{query}");
    }
}
