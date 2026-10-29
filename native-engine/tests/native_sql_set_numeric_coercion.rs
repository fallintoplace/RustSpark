use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn catalog() -> Catalog {
    let lhs = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
    let rhs = Schema::new(vec![Field::new("m", DataType::Float, true)]).unwrap();
    let text = Schema::new(vec![Field::new("v", DataType::Text, true)]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("integers", Batch::from_rows(lhs, vec![
        vec![Value::Integer(2)], vec![Value::Integer(2)],
        vec![Value::Integer(3)], vec![Value::Null],
    ]).unwrap()).unwrap();
    catalog.register("floats", Batch::from_rows(rhs, vec![
        vec![Value::Float(2.0)], vec![Value::Float(2.0)],
        vec![Value::Float(4.0)], vec![Value::Null],
    ]).unwrap()).unwrap();
    catalog.register("text", Batch::from_rows(text, vec![
        vec![Value::Text("2".into())],
    ]).unwrap()).unwrap();
    catalog
}
fn run(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    execute(&parse_sql(sql)?, &catalog())?.rows()
}
#[test]
fn spark_unions_widen_numeric_columns_before_distinct_matching() {
    assert_eq!(run("SELECT * FROM integers UNION ALL SELECT * FROM floats").unwrap(), vec![
        vec![Value::Float(2.0)], vec![Value::Float(2.0)],
        vec![Value::Float(3.0)], vec![Value::Null],
        vec![Value::Float(2.0)], vec![Value::Float(2.0)],
        vec![Value::Float(4.0)], vec![Value::Null],
    ]);
    assert_eq!(run("SELECT * FROM integers UNION SELECT * FROM floats").unwrap(), vec![
        vec![Value::Float(2.0)], vec![Value::Float(3.0)],
        vec![Value::Null], vec![Value::Float(4.0)],
    ]);
}
#[test]
fn spark_intersect_and_except_compare_coerced_numeric_keys() {
    assert_eq!(run("SELECT * FROM integers INTERSECT SELECT * FROM floats").unwrap(), vec![
        vec![Value::Float(2.0)], vec![Value::Null],
    ]);
    assert_eq!(run("SELECT * FROM integers EXCEPT SELECT * FROM floats").unwrap(), vec![
        vec![Value::Float(3.0)],
    ]);
}
#[test]
fn incompatible_text_types_remain_explicit_errors() {
    assert!(run("SELECT * FROM integers UNION SELECT * FROM text").is_err());
    assert!(run("SELECT * FROM integers INTERSECT SELECT * FROM text").is_err());
    assert!(run("SELECT * FROM integers EXCEPT SELECT * FROM text").is_err());
}
