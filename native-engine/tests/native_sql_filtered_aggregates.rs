use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("group_id", DataType::Integer, true),
            Field::new("amount", DataType::Integer, true),
            Field::new("ok", DataType::Boolean, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Integer(10), Value::Boolean(true)],
            vec![Value::Integer(1), Value::Integer(20), Value::Boolean(false)],
            vec![Value::Integer(1), Value::Null, Value::Null],
            vec![Value::Integer(2), Value::Integer(30), Value::Boolean(true)],
            vec![Value::Integer(2), Value::Integer(40), Value::Null],
        ],
    ).unwrap()).unwrap();
    c
}
fn rows(sql: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(sql).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn aggregate_filter_applies_per_item_without_removing_groups() {
    assert_eq!(rows("SELECT group_id, COUNT(*) FILTER (WHERE ok) AS n, SUM(amount) FILTER (WHERE ok) AS total FROM t GROUP BY group_id ORDER BY group_id"), vec![
        vec![Value::Integer(1), Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(2), Value::Integer(1), Value::Integer(30)],
    ]);
}

#[test]
fn filter_keeps_zero_counts_and_null_sums_on_empty_qualifying_rows() {
    assert_eq!(rows("SELECT COUNT(*) FILTER (WHERE FALSE) AS n, SUM(amount) FILTER (WHERE FALSE) AS total FROM t"), vec![
        vec![Value::Integer(0), Value::Null],
    ]);
}

#[test]
fn count_if_counts_only_true_values_not_non_null_values() {
    assert_eq!(rows("SELECT COUNT_IF(ok) AS n FROM t"), vec![vec![Value::Integer(2)]]);
    assert_eq!(rows("SELECT group_id, COUNT_IF(ok) AS n FROM t GROUP BY group_id ORDER BY group_id"), vec![
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(1)],
    ]);
}

#[test]
fn filter_and_count_distinct_maintain_correct_semantics() {
    assert_eq!(rows("SELECT COUNT(DISTINCT amount) FILTER (WHERE ok) AS n FROM t"), vec![
        vec![Value::Integer(2)],
    ]);
    assert_eq!(rows("SELECT COUNT_IF(ok) FILTER (WHERE amount > 20) FROM t"), vec![
        vec![Value::Integer(1)],
    ]);
}

#[test]
fn malformed_filters_and_type_errors_are_rejected() {
    for sql in [
        "SELECT COUNT(*) FILTER () FROM t",
        "SELECT SUM(amount) FILTER (ok) FROM t",
        "SELECT COUNT_IF(*) FROM t",
        "SELECT COUNT_IF(ok) FILTER (WHERE) FROM t",
    ] {
        assert!(parse_sql(sql).is_err(), "{sql}");
    }
    let plan = parse_sql("SELECT COUNT(*) FILTER (WHERE amount) FROM t").unwrap();
    assert!(execute(&plan, &catalog()).is_err());
}
