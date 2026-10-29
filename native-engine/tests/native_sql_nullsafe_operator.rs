use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql, lexer::{tokenize, Token},
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("a", DataType::Integer, true),
            Field::new("b", DataType::Integer, true),
        ]).unwrap(),
        vec![
            vec![Value::Null, Value::Null],
            vec![Value::Null, Value::Integer(1)],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2)],
        ],
    ).unwrap()).unwrap();
    catalog
}

fn results(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn lexer_prefers_longest_sql_comparison_operator() {
    assert!(tokenize("SELECT a <=> b FROM t").unwrap().contains(&Token::Operator("<=>".into())));
    assert!(tokenize("SELECT a == b FROM t").unwrap().contains(&Token::Operator("==".into())));
    assert!(tokenize("SELECT a <= b FROM t").unwrap().contains(&Token::Operator("<=".into())));
}

#[test]
fn spark_null_safe_equality_has_only_boolean_results() {
    assert_eq!(results("SELECT a <=> b AS equal FROM t"), vec![
        vec![Value::Boolean(true)],
        vec![Value::Boolean(false)],
        vec![Value::Boolean(false)],
        vec![Value::Boolean(true)],
        vec![Value::Boolean(false)],
    ]);
    assert_eq!(results("SELECT a = b AS equal FROM t"), vec![
        vec![Value::Null], vec![Value::Null], vec![Value::Null],
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
    ]);
}

#[test]
fn null_safe_equals_matches_is_not_distinct_from_and_equality_alias() {
    assert_eq!(results("SELECT a <=> b AS eq FROM t"),
        results("SELECT a IS NOT DISTINCT FROM b AS eq FROM t"));
    assert_eq!(results("SELECT a == b AS eq FROM t"),
        results("SELECT a = b AS eq FROM t"));
}

#[test]
fn null_safe_equality_preserves_precedence_and_predicates() {
    assert_eq!(results("SELECT a FROM t WHERE a <=> b"), vec![
        vec![Value::Null], vec![Value::Integer(1)],
    ]);
    assert_eq!(results("SELECT a <=> b AND b IS NULL FROM t"), vec![
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
        vec![Value::Boolean(false)], vec![Value::Boolean(false)],
        vec![Value::Boolean(false)],
    ]);
}

#[test]
fn null_safe_equality_handles_nan_and_signed_zero_consistently() {
    let mut catalog = Catalog::new();
    catalog.register("floats", Batch::from_rows(
        Schema::new(vec![
            Field::new("a", DataType::Float, true),
            Field::new("b", DataType::Float, true),
        ]).unwrap(),
        vec![
            vec![Value::Float(f64::NAN), Value::Float(f64::NAN)],
            vec![Value::Float(-0.0), Value::Float(0.0)],
            vec![Value::Float(1.0), Value::Float(f64::NAN)],
        ],
    ).unwrap()).unwrap();
    let got = execute(&parse_sql("SELECT a <=> b FROM floats").unwrap(), &catalog).unwrap();
    assert_eq!(got.rows().unwrap(), vec![
        vec![Value::Boolean(true)], vec![Value::Boolean(true)],
        vec![Value::Boolean(false)],
    ]);
}
