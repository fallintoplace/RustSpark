use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("customers", Batch::from_rows(
        Schema::new(vec![
            Field::new("c_id", DataType::Integer, false),
            Field::new("c_acctbal", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(30)],
            vec![Value::Integer(2), Value::Integer(10)],
            vec![Value::Integer(3), Value::Integer(50)],
            vec![Value::Integer(4), Value::Null],
        ]
    ).unwrap()).unwrap();
    c.register("benchmark", Batch::from_rows(
        Schema::new(vec![
            Field::new("b_score", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(10)],
            vec![Value::Integer(30)],
        ]
    ).unwrap()).unwrap();
    c
}

#[test]
fn tpch_q22_scalar_avg_comparison_is_uncorrelated() {
    let sql = "SELECT c_id FROM customers WHERE c_acctbal > (SELECT AVG(b_score) AS threshold FROM benchmark) ORDER BY c_id";
    assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(1)], vec![Value::Integer(3)],
    ]);
    let sql = "SELECT c_id FROM customers WHERE c_acctbal >= (SELECT AVG(b_score) AS threshold FROM benchmark) AND c_id < 3 ORDER BY c_id";
    assert_eq!(execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap(), vec![
        vec![Value::Integer(1)],
    ]);
}

#[test]
fn non_scalar_and_grouped_subqueries_fail_closed() {
    for sql in [
        "SELECT c_id FROM customers WHERE c_acctbal > (SELECT b_score FROM benchmark)",
        "SELECT c_id FROM customers WHERE c_acctbal > (SELECT AVG(b_score), COUNT(*) FROM benchmark)",
        "SELECT c_id FROM customers WHERE c_acctbal > (SELECT AVG(b_score) FROM benchmark GROUP BY b_score)",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
}
