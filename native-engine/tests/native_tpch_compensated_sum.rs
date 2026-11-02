use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn rows(sql: &str) -> Vec<Vec<Value>> {
    let mut catalog = Catalog::new();
    catalog.register("money", Batch::from_rows(
        Schema::new(vec![
            Field::new("group_id", DataType::Integer, false),
            Field::new("value", DataType::Float, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Float(1.0e16)],
            vec![Value::Integer(1), Value::Float(1.0)],
            vec![Value::Integer(1), Value::Float(-1.0e16)],
            vec![Value::Integer(2), Value::Null],
        ]
    ).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn tpch_sum_preserves_small_contributions_next_to_large_values() {
    assert_eq!(rows("SELECT SUM(value) FROM money"), vec![
        vec![Value::Float(1.0)]
    ]);
    assert_eq!(rows("SELECT group_id, SUM(value) FROM money GROUP BY group_id ORDER BY group_id"), vec![
        vec![Value::Integer(1), Value::Float(1.0)],
        vec![Value::Integer(2), Value::Null],
    ]);
}

#[test]
fn tpch_distinct_sum_uses_same_compensated_accumulator() {
    assert_eq!(rows("SELECT SUM(DISTINCT value) FROM money"), vec![
        vec![Value::Float(1.0)]
    ]);
}
