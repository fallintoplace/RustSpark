use rustspark_native::{
    catalog::Catalog, executor::execute, plan::Plan, scalar::Value, sql::parse_sql,
};

fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &Catalog::new()).unwrap().rows().unwrap()
}

#[test]
fn source_free_spark_select_executes_scalar_expressions_and_nulls() {
    assert_eq!(rows("SELECT 1 AS one, 'München' AS city, 2 * 3 AS six"), vec![
        vec![Value::Integer(1), Value::Text("München".into()), Value::Integer(6)]
    ]);
    assert_eq!(rows("SELECT CAST('42' AS INT) AS answer, NULL AS missing"), vec![
        vec![Value::Integer(42), Value::Null]
    ]);
    assert_eq!(rows("SELECT CASE WHEN FALSE THEN 8 ELSE 9 END AS bucket"), vec![
        vec![Value::Integer(9)]
    ]);
}

#[test]
fn source_free_where_filter_preserves_single_row_semantics() {
    assert_eq!(rows("SELECT 7 AS n WHERE 2 + 2 = 4"),
        vec![vec![Value::Integer(7)]]);
    assert!(rows("SELECT 7 AS n WHERE 2 + 2 = 5").is_empty());
    assert!(rows("SELECT 7 AS n WHERE NULL").is_empty());
}

#[test]
fn global_aggregation_and_set_operations_work_without_input_tables() {
    assert_eq!(rows("SELECT COUNT(*) AS n"), vec![vec![Value::Integer(1)]]);
    assert_eq!(rows("SELECT COUNT(*) AS n WHERE FALSE"), vec![vec![Value::Integer(0)]]);
    assert_eq!(rows("SELECT 1 AS n UNION ALL SELECT 2 AS n"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)]
    ]);
    assert_eq!(rows("SELECT 1 AS n INTERSECT SELECT 1 AS n"),
        vec![vec![Value::Integer(1)]]);
    assert_eq!(rows("SELECT 2 AS n ORDER BY n DESC LIMIT 1"),
        vec![vec![Value::Integer(2)]]);
}

#[test]
fn no_hidden_table_and_no_silent_from_fallback() {
    assert_eq!(parse_sql("SELECT 1").unwrap(),
        Plan::OneRow.project(vec![
            rustspark_native::project::Selection::new(
                rustspark_native::expr::Expr::Literal(Value::Integer(1)), "expr_1"
            )
        ]));
    assert!(parse_sql("SELECT *").is_err());
    assert!(parse_sql("SELECT 1 FROM").is_err());
    assert!(parse_sql("SELECT FROM").is_err());
    assert!(parse_sql("SELECT 1; SELECT 2").is_err());
}
