use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, join::{self, JoinKind}, plan::Plan,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    let left_schema = Schema::new(vec![
        Field::new("tenant", DataType::Text, true),
        Field::new("id", DataType::Integer, false),
        Field::new("payload", DataType::Integer, false),
    ]).unwrap();
    let right_schema = Schema::new(vec![
        Field::new("tenant", DataType::Text, true),
        Field::new("id", DataType::Integer, false),
        Field::new("score", DataType::Integer, false),
    ]).unwrap();
    catalog.register("a", Batch::from_rows(left_schema, vec![
        vec![Value::Text("A".into()), Value::Integer(1), Value::Integer(3)],
        vec![Value::Text("A".into()), Value::Integer(2), Value::Integer(4)],
        vec![Value::Text("B".into()), Value::Integer(1), Value::Integer(5)],
        vec![Value::Null, Value::Integer(1), Value::Integer(6)],
    ]).unwrap()).unwrap();
    catalog.register("b", Batch::from_rows(right_schema, vec![
        vec![Value::Text("A".into()), Value::Integer(1), Value::Integer(100)],
        vec![Value::Text("B".into()), Value::Integer(2), Value::Integer(200)],
        vec![Value::Text("A".into()), Value::Integer(1), Value::Integer(110)],
        vec![Value::Null, Value::Integer(1), Value::Integer(900)],
    ]).unwrap()).unwrap();
    catalog
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn composite_keys_require_both_equality_components() {
    let sql = "SELECT payload, score FROM a JOIN b ON tenant = tenant AND id = id";
    let plan = parse_sql(sql).unwrap();
    assert!(matches!(&plan, Plan::Project { input, .. }
        if matches!(input.as_ref(), Plan::MultiJoin { keys, .. } if keys.len() == 2)));
    assert_eq!(rows(sql), vec![
        vec![Value::Integer(3), Value::Integer(100)],
        vec![Value::Integer(3), Value::Integer(110)],
    ]);
}

#[test]
fn left_join_and_semi_anti_obey_composite_null_rules() {
    let left = "SELECT payload FROM a LEFT JOIN b ON tenant = tenant AND id = id";
    let left_rows = rows(left);
    assert_eq!(left_rows.len(), 5);
    assert_eq!(rows("SELECT payload FROM a LEFT SEMI JOIN b ON tenant = tenant AND id = id"),
        vec![vec![Value::Integer(3)]]);
    assert_eq!(rows("SELECT payload FROM a LEFT ANTI JOIN b ON tenant = tenant AND id = id"),
        vec![vec![Value::Integer(4)], vec![Value::Integer(5)], vec![Value::Integer(6)]]);
}

#[test]
fn single_key_join_behavior_is_preserved() {
    let plan = parse_sql("SELECT payload FROM a JOIN b ON id = id").unwrap();
    assert!(matches!(plan, Plan::Project { input, .. }
        if matches!(*input, Plan::Join { kind: JoinKind::Inner, .. })));
}

#[test]
fn engine_rejects_untyped_or_invalid_composite_key_expressions() {
    assert!(parse_sql("SELECT * FROM a JOIN b ON tenant = tenant AND id > id").is_err());
    let mut catalog = catalog();
    let right = Batch::from_rows(Schema::new(vec![
        Field::new("tenant", DataType::Integer, false),
        Field::new("id", DataType::Integer, false),
    ]).unwrap(), vec![vec![Value::Integer(1), Value::Integer(1)]]).unwrap();
    let left = catalog.get("a").unwrap();
    let error = join::hash_join_multi(left, &right,
        &[("tenant".into(), "tenant".into()), ("id".into(), "id".into())], JoinKind::Inner)
        .unwrap_err();
    assert!(error.to_string().contains("types must match"));
    // The planner does not implicitly cast unlike key types.
    catalog.register("other", right).unwrap();
    let sql = "SELECT payload FROM a JOIN other ON tenant = tenant AND id = id";
    assert!(execute(&parse_sql(sql).unwrap(), &catalog).is_err());
}

#[test]
fn explicit_many_key_limit_is_rejected() {
    let on = (0..65).map(|_| "id = id").collect::<Vec<_>>().join(" AND ");
    let sql = format!("SELECT * FROM a JOIN b ON {on}");
    assert!(parse_sql(&sql).unwrap_err().to_string().contains("64 equality keys"));
}
