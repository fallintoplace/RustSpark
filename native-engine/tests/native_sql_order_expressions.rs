use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn fixture() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(
        Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("label", DataType::Text, false),
        ]).unwrap(),
        vec![vec![Value::Integer(2), Value::Text("b".into())],
             vec![Value::Integer(4), Value::Text("A".into())],
             vec![Value::Integer(1), Value::Text("c".into())]],
    ).unwrap()).unwrap();
    catalog
}
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &fixture()).unwrap().rows().unwrap()
}
#[test]
fn computed_order_by_supports_arithmetic_functions_and_case() {
    assert_eq!(rows("SELECT n FROM data ORDER BY n * -1"), vec![
        vec![Value::Integer(4)], vec![Value::Integer(2)], vec![Value::Integer(1)]
    ]);
    assert_eq!(rows("SELECT n, label FROM data ORDER BY LOWER(label)"), vec![
        vec![Value::Integer(4), Value::Text("A".into())],
        vec![Value::Integer(2), Value::Text("b".into())],
        vec![Value::Integer(1), Value::Text("c".into())],
    ]);
    assert_eq!(rows("SELECT n FROM data ORDER BY CASE WHEN n > 2 THEN 0 ELSE 1 END, n"), vec![
        vec![Value::Integer(4)], vec![Value::Integer(1)], vec![Value::Integer(2)]
    ]);
}
#[test]
fn ordinal_and_computed_numeric_ordering_remain_distinct() {
    assert_eq!(rows("SELECT n FROM data ORDER BY 1 DESC LIMIT 1"), vec![vec![Value::Integer(4)]]);
    assert_eq!(rows("SELECT n FROM data ORDER BY n + 1 DESC LIMIT 1"), vec![vec![Value::Integer(4)]]);
    assert_eq!(rows("SELECT n FROM data ORDER BY 1 + 2 LIMIT 1"), vec![vec![Value::Integer(2)]]);
    assert!(parse_sql("SELECT n FROM data ORDER BY 0").is_err());
}
