use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn query(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let schema = Schema::new(vec![
        Field::new("group_id", DataType::Text, false),
        Field::new("flag", DataType::Boolean, true),
    ])?;
    let mut catalog = Catalog::new();
    catalog.register("flags", Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Boolean(true)],
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("a".into()), Value::Boolean(false)],
        vec![Value::Text("b".into()), Value::Boolean(true)],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Null],
    ])?)?;
    execute(&parse_sql(sql)?, &catalog)?.rows()
}

#[test]
fn grouped_boolean_aggregate_ignores_nulls() {
    assert_eq!(query("SELECT group_id, BOOL_OR(flag) AS outcome FROM flags GROUP BY group_id ORDER BY group_id").unwrap(), vec![
        vec![Value::Text("a".into()), Value::Boolean(true)],
        vec![Value::Text("b".into()), Value::Boolean(true)],
        vec![Value::Text("c".into()), Value::Null],
    ]);
}

#[test]
fn boolean_alias_having_and_type_validation() {
    assert_eq!(query("SELECT group_id, SOME(flag) AS outcome FROM flags GROUP BY group_id HAVING SOME(flag) = TRUE ORDER BY group_id").unwrap(),
        vec![vec![Value::Text("a".into()), Value::Boolean(true)], vec![Value::Text("b".into()), Value::Boolean(true)]]);
    assert!(query("SELECT BOOL_OR(group_id) FROM flags").is_err());
}
