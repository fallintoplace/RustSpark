use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn fixture() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("numbers", Batch::from_rows(
        Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
        vec![vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(1)]],
    ).unwrap()).unwrap();
    catalog
}
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &fixture()).unwrap().rows().unwrap()
}
#[test]
fn defaults_follow_spark_sql_null_ordering() {
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n"), vec![
        vec![Value::Null], vec![Value::Integer(1)], vec![Value::Integer(2)]
    ]);
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n DESC"), vec![
        vec![Value::Integer(2)], vec![Value::Integer(1)], vec![Value::Null]
    ]);
}
#[test]
fn explicit_null_order_and_topk_override_defaults() {
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n ASC NULLS LAST"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Null]
    ]);
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n DESC NULLS FIRST"), vec![
        vec![Value::Null], vec![Value::Integer(2)], vec![Value::Integer(1)]
    ]);
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n ASC LIMIT 2"), vec![
        vec![Value::Null], vec![Value::Integer(1)]
    ]);
    assert_eq!(rows("SELECT n FROM numbers ORDER BY n DESC LIMIT 2 OFFSET 1"), vec![
        vec![Value::Integer(1)], vec![Value::Null]
    ]);
}
