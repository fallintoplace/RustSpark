use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, join::{using_join, JoinKind},
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    let left = Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("tenant", DataType::Text, true),
        Field::new("name", DataType::Text, false),
    ]).unwrap();
    let right = Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("tenant", DataType::Text, true),
        Field::new("price", DataType::Integer, false),
    ]).unwrap();
    catalog.register("a", Batch::from_rows(left, vec![
        vec![Value::Integer(1), Value::Text("x".into()), Value::Text("a1".into())],
        vec![Value::Integer(2), Value::Text("y".into()), Value::Text("a2".into())],
        vec![Value::Null, Value::Text("x".into()), Value::Text("an".into())],
    ]).unwrap()).unwrap();
    catalog.register("b", Batch::from_rows(right, vec![
        vec![Value::Integer(1), Value::Text("x".into()), Value::Integer(10)],
        vec![Value::Integer(2), Value::Text("x".into()), Value::Integer(20)],
        vec![Value::Integer(3), Value::Text("z".into()), Value::Integer(30)],
        vec![Value::Null, Value::Text("x".into()), Value::Integer(40)],
    ]).unwrap()).unwrap();
    catalog
}

#[test]
fn using_joins_merge_keys_and_put_them_before_non_keys() {
    let data = execute(&parse_sql(
        "SELECT * FROM a JOIN b USING (tenant, id)"
    ).unwrap(), &catalog()).unwrap();
    let names = data.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>();
    assert_eq!(names, vec!["tenant", "id", "name", "price"]);
    assert_eq!(data.rows().unwrap(), vec![vec![
        Value::Text("x".into()), Value::Integer(1),
        Value::Text("a1".into()), Value::Integer(10),
    ]]);
}

#[test]
fn full_outer_using_coalesces_right_only_keys_without_dropping_them() {
    let result = execute(&parse_sql(
        "SELECT id, tenant, name, price FROM a FULL OUTER JOIN b USING (id, tenant)"
    ).unwrap(), &catalog()).unwrap();
    let rows = result.rows().unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.contains(&vec![
        Value::Integer(3), Value::Text("z".into()), Value::Null, Value::Integer(30),
    ]));
    assert!(rows.contains(&vec![
        Value::Integer(2), Value::Text("y".into()), Value::Text("a2".into()), Value::Null,
    ]));
    // NULL join keys do not match, but both NULL-key source rows survive.
    assert_eq!(rows.iter().filter(|row| row[0].is_null()).count(), 2);
}

#[test]
fn right_join_using_keeps_right_only_values() {
    let result = execute(&parse_sql(
        "SELECT id, price FROM a RIGHT JOIN b USING (id)"
    ).unwrap(), &catalog()).unwrap().rows().unwrap();
    assert!(result.contains(&vec![Value::Integer(3), Value::Integer(30)]));
}

#[test]
fn using_rejects_duplicate_missing_and_wrong_type_columns() {
    let catalog = catalog();
    let duplicates = parse_sql("SELECT * FROM a JOIN b USING (id, ID)").unwrap();
    assert!(execute(&duplicates, &catalog).is_err());
    let missing = parse_sql("SELECT * FROM a JOIN b USING (unknown)").unwrap();
    assert!(execute(&missing, &catalog).is_err());
    let left = catalog.get("a").unwrap();
    let right = Batch::from_rows(
        Schema::new(vec![Field::new("id", DataType::Text, false)]).unwrap(),
        vec![vec![Value::Text("1".into())]],
    ).unwrap();
    assert!(using_join(left, &right, &["id".into()], JoinKind::Inner).is_err());
}

#[test]
fn unsupported_using_forms_fail_explicitly() {
    for sql in [
        "SELECT * FROM a LEFT SEMI JOIN b USING (id)",
        "SELECT * FROM a JOIN b USING ()",
        "SELECT * FROM a JOIN b USING (id,)",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
}
