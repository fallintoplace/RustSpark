use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("partsupp", Batch::from_rows(
        Schema::new(vec![
            Field::new("ps_suppkey", DataType::Integer, false),
            Field::new("ps_supplycost", DataType::Integer, true),
            Field::new("ps_availqty", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(100), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(50), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(10), Value::Integer(5)],
        ],
    ).unwrap()).unwrap();
    c
}

fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn tpch_q11_aggregate_product_can_be_scaled_after_sum() {
    assert_eq!(rows(
        "SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 AS threshold FROM partsupp"
    ), vec![vec![Value::Float(0.035)]]);
}

#[test]
fn grouped_aggregate_expressions_use_hidden_slots_and_sort_aliases() {
    let sql = "SELECT ps_suppkey, SUM(ps_supplycost * ps_availqty) * 2 AS inventory_value FROM partsupp GROUP BY ps_suppkey ORDER BY inventory_value DESC";
    assert_eq!(rows(sql), vec![
        vec![Value::Integer(1), Value::Integer(600)],
        vec![Value::Integer(2), Value::Integer(100)],
    ]);
    assert_eq!(rows("SELECT COUNT(*) * 2 AS n FROM partsupp"),
        vec![vec![Value::Integer(6)]]);
}

#[test]
fn simple_aggregates_and_grouping_remain_unchanged() {
    assert_eq!(rows("SELECT SUM(ps_availqty) AS total FROM partsupp"),
        vec![vec![Value::Integer(9)]]);
    assert!(parse_sql(
        "SELECT ps_suppkey, SUM(ps_availqty) * 2 FROM partsupp"
    ).is_ok());
}
