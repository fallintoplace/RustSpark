use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("x", DataType::Integer, true),
            Field::new("amount", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Integer(-1), Value::Integer(10)],
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(30)],
            vec![Value::Null, Value::Integer(40)],
        ],
    ).unwrap()).unwrap();
    catalog
}

fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn grouped_function_expressions_are_evaluated_once_per_group() {
    assert_eq!(rows("SELECT ABS(x) AS bucket, SUM(amount) AS total FROM t GROUP BY ABS(x) ORDER BY bucket NULLS LAST"), vec![
        vec![Value::Integer(1), Value::Integer(30)],
        vec![Value::Integer(2), Value::Integer(30)],
        vec![Value::Null, Value::Integer(40)],
    ]);
}

#[test]
fn group_by_ordinals_and_projection_aliases_match_computed_keys() {
    let expected = vec![
        vec![Value::Integer(1), Value::Integer(30)],
        vec![Value::Integer(2), Value::Integer(30)],
        vec![Value::Null, Value::Integer(40)],
    ];
    assert_eq!(rows("SELECT ABS(x) AS bucket, SUM(amount) AS total FROM t GROUP BY 1 ORDER BY bucket NULLS LAST"), expected);
    assert_eq!(rows("SELECT ABS(x) AS bucket, SUM(amount) AS total FROM t GROUP BY bucket ORDER BY bucket NULLS LAST"), expected);
}

#[test]
fn group_by_all_infers_computed_non_aggregate_expressions() {
    let result = rows("SELECT x + 1 AS bucket, COUNT(*) AS n FROM t GROUP BY ALL ORDER BY bucket NULLS LAST");
    assert_eq!(result, vec![
        vec![Value::Integer(0), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(1)],
        vec![Value::Integer(3), Value::Integer(1)],
        vec![Value::Null, Value::Integer(1)],
    ]);
    assert_eq!(rows("SELECT COUNT(*) AS n FROM t GROUP BY ALL"), vec![vec![Value::Integer(4)]]);
}

#[test]
fn scalar_expressions_can_reference_grouped_source_columns() {
    assert_eq!(rows("SELECT x + 1 AS bucket FROM t GROUP BY x ORDER BY bucket NULLS LAST"), vec![
        vec![Value::Integer(0)], vec![Value::Integer(2)],
        vec![Value::Integer(3)], vec![Value::Null],
    ]);
}

#[test]
fn invalid_aggregate_group_keys_and_ungrouped_columns_fail() {
    assert!(parse_sql("SELECT SUM(amount) FROM t GROUP BY 1").is_err());
    assert!(parse_sql("SELECT x FROM t GROUP BY 0").is_err());
    assert!(parse_sql("SELECT x FROM t GROUP BY 3").is_err());
    let plan = parse_sql("SELECT amount, COUNT(*) FROM t GROUP BY x").unwrap();
    assert!(execute(&plan, &catalog()).is_err());
}

#[test]
fn ungrouped_aggregates_and_basic_group_by_do_not_regress() {
    assert_eq!(rows("SELECT SUM(amount) AS total FROM t"), vec![vec![Value::Integer(100)]]);
    assert_eq!(rows("SELECT x, COUNT(*) AS count FROM t GROUP BY x ORDER BY x NULLS LAST"), vec![
        vec![Value::Integer(-1), Value::Integer(1)],
        vec![Value::Integer(1), Value::Integer(1)],
        vec![Value::Integer(2), Value::Integer(1)],
        vec![Value::Null, Value::Integer(1)],
    ]);
}
