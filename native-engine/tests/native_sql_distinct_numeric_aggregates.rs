use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(query: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("group_id", DataType::Integer, false),
        Field::new("amount", DataType::Integer, true),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("sales", Batch::from_rows(schema, vec![
        vec![Value::Integer(1), Value::Integer(2)],
        vec![Value::Integer(1), Value::Integer(2)],
        vec![Value::Integer(1), Value::Integer(6)],
        vec![Value::Integer(1), Value::Null],
        vec![Value::Integer(2), Value::Null],
        vec![Value::Integer(2), Value::Null],
        vec![Value::Integer(3), Value::Integer(-4)],
        vec![Value::Integer(3), Value::Integer(-4)],
    ]).unwrap()).unwrap();
    execute(&parse_sql(query).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn spark_distinct_sum_and_avg_deduplicate_before_aggregation() {
    let rows = run(
        "SELECT group_id, SUM(DISTINCT amount) AS sum_unique,          AVG(DISTINCT amount) AS avg_unique, SUM(amount) AS sum_all, AVG(amount) AS avg_all          FROM sales GROUP BY group_id ORDER BY group_id"
    );
    assert_eq!(rows, vec![
        vec![Value::Integer(1), Value::Integer(8), Value::Float(4.0),
             Value::Integer(10), Value::Float(10.0/3.0)],
        vec![Value::Integer(2), Value::Null, Value::Null, Value::Null, Value::Null],
        vec![Value::Integer(3), Value::Integer(-4), Value::Float(-4.0),
             Value::Integer(-8), Value::Float(-4.0)],
    ]);
}

#[test]
fn spark_distinct_aggregates_support_global_groups_and_nulls() {
    assert_eq!(run(
        "SELECT SUM(DISTINCT amount) AS total, AVG(DISTINCT amount) AS avg FROM sales"
    ), vec![vec![Value::Integer(4), Value::Float(4.0 / 3.0)]]);
    assert!(parse_sql("SELECT MAX(DISTINCT amount) FROM sales").is_err());
    assert!(parse_sql("SELECT AVG(DISTINCT amount, amount) FROM sales").is_err());
}

#[test]
fn spark_distinct_sum_preserves_checked_integer_overflow() {
    let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(schema, vec![
        vec![Value::Integer(i64::MAX)], vec![Value::Integer(1)]
    ]).unwrap()).unwrap();
    assert!(execute(
        &parse_sql("SELECT SUM(DISTINCT n) FROM t").unwrap(), &catalog
    ).is_err());
}
