use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn query(sql: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("name", DataType::Text, true),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("sample", Batch::from_rows(schema, vec![
        vec![Value::Integer(1), Value::Text("alpha".into())],
        vec![Value::Integer(2), Value::Text("beta".into())],
        vec![Value::Integer(3), Value::Null],
        vec![Value::Null, Value::Text("beta".into())],
    ]).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn spark_simple_case_handles_ints_text_and_default_branches() {
    assert_eq!(query(
        "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS label FROM sample"
    ), vec![
        vec![Value::Text("one".into())], vec![Value::Text("two".into())],
        vec![Value::Text("other".into())], vec![Value::Text("other".into())],
    ]);
    assert_eq!(query(
        "SELECT CASE name WHEN 'beta' THEN 10 WHEN 'alpha' THEN 20 END AS score FROM sample"
    ), vec![
        vec![Value::Integer(20)], vec![Value::Integer(10)],
        vec![Value::Null], vec![Value::Integer(10)],
    ]);
}

#[test]
fn simple_case_uses_sql_null_comparison_and_short_circuits() {
    assert_eq!(query(
        "SELECT CASE id WHEN NULL THEN 9 WHEN 1 THEN 1 ELSE 0 END AS n FROM sample"
    ), vec![
        vec![Value::Integer(1)], vec![Value::Integer(0)],
        vec![Value::Integer(0)], vec![Value::Integer(0)],
    ]);
    assert_eq!(query(
        "SELECT CASE id WHEN 1 THEN 5 WHEN 2 THEN 6 ELSE 7 END AS n FROM sample WHERE id = 1"
    ), vec![vec![Value::Integer(5)]]);
    assert_eq!(query(
        "SELECT CASE id WHEN 1 THEN 5 WHEN 2 THEN 9223372036854775807 + 1          ELSE 7 END AS n FROM sample WHERE id = 1"
    ), vec![vec![Value::Integer(5)]]);
}

#[test]
fn searched_case_remains_supported_and_invalid_case_rejected() {
    assert_eq!(query(
        "SELECT CASE WHEN id IS NULL THEN 9 WHEN id > 1 THEN 2 ELSE 1 END AS n FROM sample"
    ), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
        vec![Value::Integer(2)], vec![Value::Integer(9)],
    ]);
    assert!(parse_sql("SELECT CASE id ELSE 2 END FROM sample").is_err());
    assert!(parse_sql("SELECT CASE WHEN id THEN 2 FROM sample").is_err());
}
