use rustspark_native::{
    batch::Batch, catalog::Catalog, executor,
    scalar::Value, schema::{DataType, Field, Schema}, sql,
};

fn run(query: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![Field::new("txt", DataType::Text, true)]).unwrap();
    let data = Batch::from_rows(schema, vec![
        vec![Value::Text("foobarbar".into())],
        vec![Value::Text("café".into())],
        vec![Value::Null],
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("t", data).unwrap();
    executor::execute(&sql::parse_sql(query).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn ansi_position_in_accepts_literal_and_column_expressions() {
    assert_eq!(run("SELECT POSITION('bar' IN txt) FROM t"), vec![
        vec![Value::Integer(4)], vec![Value::Integer(0)], vec![Value::Null],
    ]);
    assert_eq!(run("SELECT POSITION('é' IN txt) FROM t")[1], vec![Value::Integer(4)]);
}

#[test]
fn spark_position_function_accepts_start_offset() {
    assert_eq!(run("SELECT POSITION('bar', txt, 5) FROM t")[0], vec![Value::Integer(7)]);
    assert_eq!(run("SELECT POSITION('bar', txt) FROM t")[0], vec![Value::Integer(4)]);
    assert!(sql::parse_sql("SELECT POSITION('x' IN) FROM t").is_err());
}
