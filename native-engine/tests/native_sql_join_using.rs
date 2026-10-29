use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
fn tables() -> Catalog {
    let lhs = Schema::new(vec![
        Field::new("item", DataType::Text, false),
        Field::new("id", DataType::Integer, true),
        Field::new("region", DataType::Text, false),
    ]).unwrap();
    let rhs = Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("region", DataType::Text, false),
        Field::new("item", DataType::Text, false),
    ]).unwrap();
    let mut c = Catalog::new();
    c.register("l", Batch::from_rows(lhs, vec![
        vec![Value::Text("a".into()), Value::Integer(1), Value::Text("de".into())],
        vec![Value::Text("b".into()), Value::Integer(1), Value::Text("us".into())],
        vec![Value::Text("c".into()), Value::Integer(2), Value::Text("de".into())],
        vec![Value::Text("null".into()), Value::Null, Value::Text("de".into())],
    ]).unwrap()).unwrap();
    c.register("r", Batch::from_rows(rhs, vec![
        vec![Value::Integer(1), Value::Text("de".into()), Value::Text("x".into())],
        vec![Value::Integer(1), Value::Text("fr".into()), Value::Text("y".into())],
        vec![Value::Integer(3), Value::Text("us".into()), Value::Text("z".into())],
        vec![Value::Null, Value::Text("de".into()), Value::Text("null".into())],
    ]).unwrap()).unwrap();
    c
}
fn run(query: &str) -> Batch {
    execute(&parse_sql(query).unwrap(), &tables()).unwrap()
}
#[test]
fn using_join_deduplicates_composite_keys_and_reorders_columns() {
    let result = run("SELECT * FROM l JOIN r USING (id, region)");
    assert_eq!(result.schema().fields().iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "region", "item", "right_item"]);
    assert_eq!(result.rows().unwrap(), vec![vec![
        Value::Integer(1), Value::Text("de".into()),
        Value::Text("a".into()), Value::Text("x".into()),
    ]]);
}
#[test]
fn full_right_semi_anti_using_match_spark_null_semantics() {
    let full = run("SELECT * FROM l FULL OUTER JOIN r USING (id, region)");
    assert_eq!(full.num_rows(), 7);
    let rows = full.rows().unwrap();
    assert!(rows.contains(&vec![
        Value::Integer(3), Value::Text("us".into()), Value::Null, Value::Text("z".into())
    ]));
    assert!(rows.contains(&vec![
        Value::Null, Value::Text("de".into()), Value::Null, Value::Text("null".into())
    ]));
    assert_eq!(run("SELECT * FROM l RIGHT JOIN r USING (id, region)").num_rows(), 4);
    assert_eq!(run("SELECT * FROM l LEFT SEMI JOIN r USING (id, region)").num_rows(), 1);
    let anti = run("SELECT * FROM l LEFT ANTI JOIN r USING (id, region)");
    assert_eq!(anti.num_rows(), 3);
    assert_eq!(anti.schema().len(), 3);
}
#[test]
fn malformed_keys_fail_instead_of_joining_all_rows() {
    for q in [
        "SELECT * FROM l JOIN r USING ()",
        "SELECT * FROM l JOIN r USING (id, id)",
        "SELECT * FROM l JOIN r USING (id, )",
    ] { assert!(parse_sql(q).is_err(), "{q}"); }
}
