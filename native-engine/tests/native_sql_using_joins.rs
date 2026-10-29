use rustspark_native::{
    batch::Batch,catalog::Catalog,executor::execute,scalar::Value,
    schema::{DataType,Field,Schema},sql::parse_sql,
};
fn fixtures()->Catalog{
    let mut c=Catalog::new();
    c.register("orders",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("amount",DataType::Integer,false),
    ]).unwrap(),vec![
        vec![Value::Integer(1),Value::Integer(10)],
        vec![Value::Integer(2),Value::Integer(20)]
    ]).unwrap()).unwrap();
    c.register("payments",Batch::from_rows(Schema::new(vec![
        Field::new("id",DataType::Integer,false),
        Field::new("state",DataType::Text,false),
    ]).unwrap(),vec![
        vec![Value::Integer(2),Value::Text("paid".into())],
        vec![Value::Integer(3),Value::Text("late".into())],
    ]).unwrap()).unwrap();
    c
}
#[test]
fn using_inner_join_merges_key_columns_and_orders_payloads() {
    let rows=execute(&parse_sql("SELECT * FROM orders JOIN payments USING (id)").unwrap(),&fixtures())
        .unwrap();
    assert_eq!(rows.schema().fields().iter().map(|f|f.name.as_str()).collect::<Vec<_>>(),
        vec!["id","amount","state"]);
    assert_eq!(rows.rows().unwrap(),vec![vec![Value::Integer(2),
        Value::Integer(20),Value::Text("paid".into())]]);
}
#[test]
fn using_full_join_coalesces_unmatched_right_keys() {
    let rows=execute(&parse_sql("SELECT * FROM orders FULL OUTER JOIN payments USING (id)").unwrap(),&fixtures())
        .unwrap().rows().unwrap();
    assert_eq!(rows,vec![
        vec![Value::Integer(1),Value::Integer(10),Value::Null],
        vec![Value::Integer(2),Value::Integer(20),Value::Text("paid".into())],
        vec![Value::Integer(3),Value::Null,Value::Text("late".into())],
    ]);
}
#[test]
fn using_rejects_ambiguous_duplicate_key_names() {
    assert!(parse_sql("SELECT * FROM orders JOIN payments USING (id, ID)").is_err());
    let result=execute(&parse_sql("SELECT * FROM orders JOIN payments USING (missing)").unwrap(),&fixtures());
    assert!(result.is_err());
}
