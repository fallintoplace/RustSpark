use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

#[test]
fn union_all_applies_global_order_and_limit() {
    let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("a", Batch::from_rows(schema.clone(), vec![
        vec![Value::Integer(4)], vec![Value::Integer(2)],
    ]).unwrap()).unwrap();
    catalog.register("b", Batch::from_rows(schema, vec![
        vec![Value::Integer(3)], vec![Value::Integer(1)],
    ]).unwrap()).unwrap();
    let plan = parse_sql(
        "SELECT * FROM a UNION ALL SELECT * FROM b ORDER BY id ASC LIMIT 2"
    ).unwrap();
    let result = execute(&plan, &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Integer(1)], vec![Value::Integer(2)],
    ]);
    assert!(parse_sql("SELECT * FROM a UNION SELECT * FROM b").is_err());
}
