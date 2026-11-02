use rustspark_native::{
    batch::Batch, catalog::Catalog, executor, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};
#[test]
fn table_query_returns_registered_rows_and_supports_limits() {
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
    catalog.register("numbers", Batch::from_rows(schema, vec![
        vec![Value::Integer(2)], vec![Value::Integer(1)],
    ]).unwrap()).unwrap();
    let plan = parse_sql("TABLE numbers ORDER BY id LIMIT 1").unwrap();
    assert_eq!(executor::execute(&plan, &catalog).unwrap().rows().unwrap(),
        vec![vec![Value::Integer(1)]]);
    assert!(executor::execute(&parse_sql("TABLE missing").unwrap(), &catalog).is_err());
}
