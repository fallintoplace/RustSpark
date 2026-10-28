use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

#[test]
fn native_join_runs_from_sql_to_rows() {
    let mut catalog = Catalog::new();
    catalog.register("orders", Batch::from_rows(
        Schema::new(vec![
            Field::new("customer", DataType::Integer, true),
            Field::new("value", DataType::Integer, false),
        ]).unwrap(),
        vec![
            vec![Value::Integer(3), Value::Integer(9)],
            vec![Value::Null, Value::Integer(4)],
        ],
    ).unwrap()).unwrap();
    catalog.register("customers", Batch::from_rows(
        Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("name", DataType::Text, false),
        ]).unwrap(),
        vec![vec![Value::Integer(3), Value::Text("Ada".into())]],
    ).unwrap()).unwrap();

    let joined = execute(&parse_sql(
        "SELECT * FROM orders LEFT JOIN customers ON customer = id"
    ).unwrap(), &catalog).unwrap();
    assert_eq!(joined.num_rows(), 2);
    assert_eq!(joined.row(0).unwrap()[3], Value::Text("Ada".into()));
    assert_eq!(joined.row(1).unwrap()[3], Value::Null);
    let inner = execute(&parse_sql(
        "SELECT * FROM orders INNER JOIN customers ON customer = id"
    ).unwrap(), &catalog).unwrap();
    assert_eq!(inner.num_rows(), 1);
    assert!(parse_sql("SELECT * FROM orders JOIN customers ON customer > id").is_err());
}
