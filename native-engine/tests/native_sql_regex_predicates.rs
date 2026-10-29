use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("names", Batch::from_rows(
        Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap(),
        vec![
            vec![Value::Text("Alice".into())],
            vec![Value::Text("BÖB".into())],
            vec![Value::Text("A123".into())],
            vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    catalog
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn rlike_and_regexp_execute_as_sql_predicates() {
    let expected = vec![
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
        vec![Value::Boolean(true)], vec![Value::Null],
    ];
    assert_eq!(rows("SELECT name RLIKE '^A.*' AS matches FROM names"), expected);
    assert_eq!(rows("SELECT name REGEXP '^A.*' AS matches FROM names"), expected);
    assert_eq!(rows("SELECT REGEXP_LIKE(name, '^A.*') AS matches FROM names"), expected);
}

#[test]
fn negated_regex_preserves_null_and_filter_semantics() {
    assert_eq!(rows("SELECT name NOT RLIKE '^A' FROM names"), vec![
        vec![Value::Boolean(false)], vec![Value::Boolean(true)],
        vec![Value::Boolean(false)], vec![Value::Null],
    ]);
    assert_eq!(rows("SELECT name FROM names WHERE name REGEXP '^[A-Z]+'"), vec![
        vec![Value::Text("Alice".into())],
        vec![Value::Text("BÖB".into())],
        vec![Value::Text("A123".into())],
    ]);
    assert_eq!(rows("SELECT name NOT REGEXP '^A' FROM names"),
        rows("SELECT NOT (name REGEXP '^A') FROM names"));
}

#[test]
fn regex_match_is_search_not_full_text_equality() {
    assert_eq!(rows("SELECT name RLIKE '[0-9]+' FROM names"), vec![
        vec![Value::Boolean(false)], vec![Value::Boolean(false)],
        vec![Value::Boolean(true)], vec![Value::Null],
    ]);
}

#[test]
fn regex_predicate_precedence_matches_like() {
    assert_eq!(rows("SELECT name RLIKE '^A' AND name LIKE 'A%' FROM names"), vec![
        vec![Value::Boolean(true)], vec![Value::Boolean(false)],
        vec![Value::Boolean(true)], vec![Value::Null],
    ]);
}

#[test]
fn malformed_regex_and_bad_argument_types_do_not_succeed_silently() {
    assert!(execute(&parse_sql("SELECT name RLIKE '[' FROM names").unwrap(),
        &catalog()).is_err());
    assert!(execute(&parse_sql("SELECT name RLIKE 10 FROM names").unwrap(),
        &catalog()).is_err());
    assert!(parse_sql("SELECT name NOT RLIKE FROM names").is_err());
}
