use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
        vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
            vec![Value::Integer(3)], vec![Value::Null],
        ],
    ).unwrap()).unwrap();
    catalog
}
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn simple_case_matches_values_and_falls_back_to_else() {
    assert_eq!(rows("SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS bucket FROM t"), vec![
        vec![Value::Text("one".into())],
        vec![Value::Text("two".into())],
        vec![Value::Text("other".into())],
        vec![Value::Text("other".into())],
    ]);
}

#[test]
fn simple_case_without_else_propagates_null_for_non_matches() {
    assert_eq!(rows("SELECT CASE x WHEN 2 THEN 9 END FROM t"), vec![
        vec![Value::Null], vec![Value::Integer(9)],
        vec![Value::Null], vec![Value::Null],
    ]);
}

#[test]
fn simple_case_uses_sql_null_matching_and_not_null_safe_equality() {
    assert_eq!(rows("SELECT CASE x WHEN NULL THEN 1 WHEN 1 THEN 2 ELSE 3 END FROM t"), vec![
        vec![Value::Integer(2)], vec![Value::Integer(3)],
        vec![Value::Integer(3)], vec![Value::Integer(3)],
    ]);
}

#[test]
fn expressions_are_supported_in_base_and_when_branches() {
    assert_eq!(rows("SELECT CASE x + 1 WHEN 2 THEN 20 WHEN 3 THEN 30 ELSE 0 END FROM t"), vec![
        vec![Value::Integer(20)], vec![Value::Integer(30)],
        vec![Value::Integer(0)], vec![Value::Integer(0)],
    ]);
}

#[test]
fn searched_case_still_short_circuits_and_has_identical_results() {
    assert_eq!(rows("SELECT CASE WHEN x = 1 THEN 10 WHEN x = 2 THEN 20 ELSE 30 END FROM t"), vec![
        vec![Value::Integer(10)], vec![Value::Integer(20)],
        vec![Value::Integer(30)], vec![Value::Integer(30)],
    ]);
    assert!(execute(&parse_sql("SELECT CASE WHEN x = 1 THEN 1 ELSE 1 / 0 END FROM t").unwrap(), &catalog()).is_err());
}

#[test]
fn malformed_case_and_mixed_return_types_fail_explicitly() {
    for query in [
        "SELECT CASE x END FROM t",
        "SELECT CASE WHEN x = 1 THEN 1 FROM t",
        "SELECT CASE x WHEN 1 ELSE 2 END FROM t",
    ] {
        assert!(parse_sql(query).is_err(), "{query}");
    }
    let plan = parse_sql("SELECT CASE x WHEN 1 THEN 'one' ELSE 2 END FROM t").unwrap();
    assert!(execute(&plan, &catalog()).is_err());
}
