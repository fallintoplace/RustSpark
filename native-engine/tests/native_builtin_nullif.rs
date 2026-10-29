use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute, scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};

#[test]
fn native_nullif_runs_from_sql_without_datafusion() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Integer, true),
        Field::new("b", DataType::Integer, false),
        Field::new("txt", DataType::Text, true),
        Field::new("suffix", DataType::Text, false),
        Field::new("z", DataType::Float, false),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(schema, vec![
        vec![Value::Integer(-7), Value::Integer(3), Value::Text(" HeLLo ".into()), Value::Text("lo".into()), Value::Float(4.0)],
        vec![Value::Null, Value::Integer(2), Value::Null, Value::Text("x".into()), Value::Float(0.0)],
    ]).unwrap()).unwrap();
    let output = execute(&parse_sql("SELECT NULLIF(b, 3) AS v FROM data").unwrap(), &catalog).unwrap();
    assert_eq!(output.row(0).unwrap(), vec![Value::Null]);
    assert_eq!(output.row(1).unwrap(), vec![Value::Integer(2)]);
}
