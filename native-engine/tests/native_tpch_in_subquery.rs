use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("orders", Batch::from_rows(
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Integer, true),
            Field::new("o_total", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(30)],
            vec![Value::Integer(3), Value::Integer(40)],
            vec![Value::Null, Value::Integer(50)],
        ]
    ).unwrap()).unwrap();
    c.register("lineitem", Batch::from_rows(
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Integer, true),
            Field::new("l_quantity", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(2), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(250)],
            vec![Value::Integer(3), Value::Integer(10)],
            vec![Value::Null, Value::Integer(500)],
        ]
    ).unwrap()).unwrap();
    c
}

#[test]
fn tpch_q18_in_grouped_subquery_preserves_duplicates() {
    let sql = "SELECT o_total FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) ORDER BY o_total";
    assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(20)], vec![Value::Integer(30)],
    ]);
}

#[test]
fn in_subquery_validates_shape_and_keeps_and_suffix() {
    let sql = "SELECT o_total FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem) AND o_total >= 30 ORDER BY o_total";
    assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(30)], vec![Value::Integer(40)],
    ]);
    assert!(parse_sql("SELECT o_total FROM orders WHERE o_orderkey IN (SELECT l_orderkey, l_quantity FROM lineitem)").is_err());
    assert!(parse_sql("SELECT o_total FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)").is_err());
}
