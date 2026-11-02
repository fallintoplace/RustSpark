use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("customers", Batch::from_rows(
        Schema::new(vec![Field::new("c_id", DataType::Integer, false)]).unwrap(),
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)],
        ]
    ).unwrap()).unwrap();
    c.register("orders", Batch::from_rows(
        Schema::new(vec![
            Field::new("o_custkey", DataType::Integer, false),
            Field::new("o_comment", DataType::Text, false),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text("regular order".into())],
            vec![Value::Integer(1), Value::Text("special rush requests".into())],
            vec![Value::Integer(2), Value::Text("special extra requests".into())],
        ]
    ).unwrap()).unwrap();
    c
}

#[test]
fn tpch_q13_left_join_right_only_on_predicate_keeps_unmatched_customers() {
    let sql = "SELECT c_id, COUNT(o_custkey) AS orders_count FROM customers LEFT JOIN orders ON c_id = o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_id ORDER BY c_id";
    assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(0)],
        vec![Value::Integer(3), Value::Integer(0)],
    ]);
}

#[test]
fn left_on_predicate_that_uses_only_outer_columns_fails_instead_of_moving_to_where() {
    let plan = parse_sql(
        "SELECT * FROM customers LEFT JOIN orders ON c_id = o_custkey AND c_id > 1"
    ).unwrap();
    assert!(execute(&plan, &catalog()).is_err());
}

#[test]
fn full_join_residual_is_still_explicitly_unsupported() {
    assert!(parse_sql(
        "SELECT * FROM customers FULL JOIN orders ON c_id = o_custkey AND o_comment != 'x'"
    ).is_err());
}
