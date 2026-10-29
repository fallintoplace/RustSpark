use rustspark_native::{
    batch::Batch, catalog::Catalog, executor, scalar::Value,
    schema::{DataType, Field, Schema}, sql,
};

fn run(query: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("number", DataType::Integer, true),
        Field::new("name", DataType::Text, true),
    ]).unwrap();
    let rows = vec![
        vec![Value::Null, Value::Null],
        vec![Value::Integer(3), Value::Text("row".into())],
    ];
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(schema, rows).unwrap()).unwrap();
    executor::execute(&sql::parse_sql(query).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn typeof_uses_static_schema_even_for_null_column_values() {
    assert_eq!(run("SELECT TYPEOF(number), TYPEOF(name) FROM t"), vec![
        vec![Value::Text("bigint".into()), Value::Text("string".into())],
        vec![Value::Text("bigint".into()), Value::Text("string".into())],
    ]);
}

#[test]
fn typeof_classifies_literals_casts_and_explicit_nulls() {
    assert_eq!(run("SELECT TYPEOF(NULL), TYPEOF(TRUE), TYPEOF(2.5), TYPEOF(CAST(number AS DOUBLE)) FROM t")[0],
        vec![
            Value::Text("void".into()), Value::Text("boolean".into()),
            Value::Text("double".into()), Value::Text("double".into()),
        ]);
    assert!(sql::parse_sql("SELECT TYPEOF() FROM t").is_ok());
}
