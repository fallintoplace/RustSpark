use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("parts", Batch::from_rows(Schema::new(vec![
        Field::new("p_id", DataType::Integer, false),
        Field::new("p_size", DataType::Integer, false),
    ]).unwrap(), vec![
        vec![Value::Integer(1), Value::Integer(3)],
        vec![Value::Integer(2), Value::Integer(5)],
    ]).unwrap()).unwrap();
    c.register("supplies", Batch::from_rows(Schema::new(vec![
        Field::new("s_id", DataType::Integer, false),
        Field::new("s_price", DataType::Integer, false),
    ]).unwrap(), vec![
        vec![Value::Integer(1), Value::Integer(100)],
        vec![Value::Integer(2), Value::Integer(20)],
    ]).unwrap()).unwrap();
    c
}

#[test]
fn tpch_inner_join_on_equality_and_residual_predicate() {
    let rows = execute(&parse_sql(
        "SELECT p_id FROM parts INNER JOIN supplies ON p_id = s_id AND p_size > 2 AND s_price < 50 ORDER BY p_id"
    ).unwrap(), &catalog()).unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
}

#[test]
fn outer_join_residual_is_rejected_instead_of_filtered_after_null_extension() {
    for join in ["LEFT", "RIGHT", "FULL"] {
        let sql = format!(
            "SELECT * FROM parts {join} JOIN supplies ON p_id = s_id AND s_price < 50"
        );
        assert!(parse_sql(&sql).is_err(), "{sql}");
    }
}
