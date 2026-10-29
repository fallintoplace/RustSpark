use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn fixtures() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("orders", Batch::from_rows(
        Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("amount", DataType::Integer, true),
        ]).unwrap(),
        vec![vec![Value::Integer(1), Value::Integer(10)],
             vec![Value::Integer(2), Value::Integer(20)]],
    ).unwrap()).unwrap();
    catalog.register("payments", Batch::from_rows(
        Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("label", DataType::Text, false),
        ]).unwrap(),
        vec![vec![Value::Integer(2), Value::Text("paid".into())],
             vec![Value::Integer(3), Value::Text("late".into())]],
    ).unwrap()).unwrap();
    catalog
}

#[test]
fn joined_qualified_columns_keep_each_side_distinct() {
    let sql = "SELECT l.id AS order_id, r.id AS payment_id, l.amount, r.label \
               FROM orders AS l LEFT JOIN payments r ON l.id = r.id ORDER BY order_id";
    let output = execute(&parse_sql(sql).unwrap(), &fixtures()).unwrap();
    assert_eq!(output.rows().unwrap(), vec![
        vec![Value::Integer(1), Value::Null, Value::Integer(10), Value::Null],
        vec![Value::Integer(2), Value::Integer(2), Value::Integer(20), Value::Text("paid".into())]
    ]);
    assert_eq!(output.schema().fields()[0].name, "order_id");
    assert_eq!(output.schema().fields()[2].name, "amount");
}

#[test]
fn unqualified_columns_resolve_only_when_unambiguous() {
    let alias = execute(&parse_sql(
        "SELECT id, amount FROM orders l WHERE l.id = 2"
    ).unwrap(), &fixtures()).unwrap();
    assert_eq!(alias.rows().unwrap(), vec![vec![Value::Integer(2), Value::Integer(20)]]);
    let ambiguous = parse_sql(
        "SELECT id FROM orders l JOIN payments r ON l.id = r.id"
    ).unwrap();
    assert!(execute(&ambiguous, &fixtures()).is_err());
    let qualified = parse_sql(
        "SELECT l.id AS left_id, r.id AS right_id FROM orders l JOIN payments r ON l.id = r.id"
    ).unwrap();
    assert_eq!(execute(&qualified, &fixtures()).unwrap().num_rows(), 1);
}
