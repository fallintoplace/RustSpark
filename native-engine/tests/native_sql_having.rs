use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

#[test]
fn having_filters_computed_group_totals() {
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(
        Schema::new(vec![
            Field::new("city", DataType::Text, false),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap(),
        vec![
            vec![Value::Text("Berlin".into()), Value::Integer(4)],
            vec![Value::Text("Berlin".into()), Value::Integer(5)],
            vec![Value::Text("Paris".into()), Value::Integer(2)],
        ],
    ).unwrap()).unwrap();
    let result = execute(&parse_sql(
        "SELECT city, SUM(amount) AS total FROM sales GROUP BY city HAVING total > 8"
    ).unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Text("Berlin".into()), Value::Integer(9)],
    ]);
    assert!(parse_sql("SELECT * FROM sales HAVING amount > 1").is_err());
}
