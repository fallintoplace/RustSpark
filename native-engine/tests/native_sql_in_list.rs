use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("t", Batch::from_rows(
        Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap(),
        vec![
            vec![Value::Null], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Integer(5)],
        ],
    ).unwrap()).unwrap();
    c
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn in_list_returns_sql_three_valued_boolean() {
    assert_eq!(rows("SELECT id IN (1, 3, NULL) FROM t"), vec![
        vec![Value::Null], vec![Value::Boolean(true)],
        vec![Value::Null], vec![Value::Null],
    ]);
    assert_eq!(rows("SELECT id NOT IN (1, 3, NULL) FROM t"), vec![
        vec![Value::Null], vec![Value::Boolean(false)],
        vec![Value::Null], vec![Value::Null],
    ]);
}

#[test]
fn duplicate_candidates_and_numeric_coercion_do_not_change_membership() {
    assert_eq!(rows("SELECT id IN (1, 1, 2.0) FROM t"), vec![
        vec![Value::Null], vec![Value::Boolean(true)],
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
    ]);
}

#[test]
fn filtered_in_and_not_in_query_results_are_correct() {
    assert_eq!(rows("SELECT id FROM t WHERE id IN (1, 2, NULL)"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
    ]);
    assert_eq!(rows("SELECT id FROM t WHERE id NOT IN (1, 2)"), vec![
        vec![Value::Integer(5)],
    ]);
}

#[test]
fn parser_produces_one_bounded_predicate_not_recursive_or_tree() {
    let plan = parse_sql("SELECT id IN (1, 2, 3) AS match_id FROM t").unwrap();
    let debug = format!("{plan:?}");
    assert!(debug.contains("InList"));
    assert!(!debug.contains("Binary { left: Binary"));
    assert!(parse_sql("SELECT id IN () FROM t").is_err());
    assert!(parse_sql("SELECT id IN (1,) FROM t").is_err());
    assert!(parse_sql("SELECT id NOT IN () FROM t").is_err());
}

#[test]
fn many_values_are_bounded_and_do_not_overflow_expression_depth() {
    let list = (1..=512).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
    let sql = format!("SELECT id IN ({list}) FROM t");
    let plan = parse_sql(&sql).unwrap();
    let rows = execute(&plan, &catalog()).unwrap().rows().unwrap();
    assert_eq!(rows[1], vec![Value::Boolean(true)]);
    assert_eq!(rows[3], vec![Value::Boolean(true)]);
    let too_many = "1,".repeat(4096) + "1";
    let sql = format!("SELECT id IN ({too_many}) FROM t");
    assert!(parse_sql(&sql).is_err());
}

#[test]
fn incompatible_types_are_rejected() {
    let plan = parse_sql("SELECT id IN ('a') FROM t").unwrap();
    assert!(execute(&plan, &catalog()).is_err());
    let empty = Batch::empty(catalog().get("t").unwrap().schema().clone());
    assert!(Expr::InList {
        needle: Box::new(Expr::Column("id".into())),
        choices: vec![Expr::Literal(Value::Text("1".into()))],
        negated: false,
    }.output_type(empty.schema()).is_err());
}
