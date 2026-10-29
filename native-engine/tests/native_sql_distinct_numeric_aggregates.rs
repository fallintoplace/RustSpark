use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(3)],
        ],
    ).unwrap()).unwrap();
    catalog
}
fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn distinct_sum_and_avg_ignore_repeated_values_and_nulls() {
    assert_eq!(rows("SELECT g, SUM(DISTINCT n) AS total, AVG(DISTINCT n) AS average FROM t GROUP BY g ORDER BY g"), vec![
        vec![Value::Integer(1), Value::Integer(3), Value::Float(1.5)],
        vec![Value::Integer(2), Value::Integer(4), Value::Float(2.0)],
    ]);
}

#[test]
fn distinct_and_nondistinct_aggregates_are_independent() {
    assert_eq!(rows("SELECT SUM(n) AS raw, SUM(DISTINCT n) AS distinct_total, AVG(n) AS mean, AVG(DISTINCT n) AS distinct_mean FROM t"), vec![
        vec![Value::Integer(9), Value::Integer(7), Value::Float(1.8), Value::Float(2.0)],
    ]);
}

#[test]
fn empty_inputs_produce_null_sum_and_avg_not_zero() {
    assert_eq!(rows("SELECT SUM(DISTINCT n), AVG(DISTINCT n), COUNT(DISTINCT n) FROM t WHERE FALSE"), vec![
        vec![Value::Null, Value::Null, Value::Integer(0)],
    ]);
}

#[test]
fn filtered_distinct_aggregates_preserve_filter_semantics() {
    assert_eq!(rows("SELECT SUM(DISTINCT n) FILTER (WHERE n > 1) FROM t"), vec![
        vec![Value::Integer(5)],
    ]);
}

#[test]
fn nonnumeric_distinct_arguments_fail_typed_aggregate_validation() {
    let mut catalog = catalog();
    catalog.register("texts", Batch::from_rows(
        Schema::new(vec![Field::new("s", DataType::Text, true)]).unwrap(),
        vec![vec![Value::Text("x".into())]],
    ).unwrap()).unwrap();
    let plan = parse_sql("SELECT AVG(DISTINCT s) FROM texts").unwrap();
    assert!(execute(&plan, &catalog).is_err());
    assert!(parse_sql("SELECT MIN(DISTINCT n) FROM t").is_err());
}
