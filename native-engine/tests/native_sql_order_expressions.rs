use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register("t", Batch::from_rows(
        Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("score", DataType::Integer, true),
        ]).unwrap(),
        vec![
            vec![Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Integer(10)],
            vec![Value::Integer(4), Value::Integer(10)],
        ],
    ).unwrap()).unwrap();
    catalog
}

fn rows(query: &str) -> Vec<Vec<Value>> {
    execute(&parse_sql(query).unwrap(), &catalog()).unwrap().rows().unwrap()
}

#[test]
fn ordering_by_non_projected_columns_matches_spark() {
    assert_eq!(rows("SELECT id FROM t ORDER BY score ASC NULLS LAST, id DESC"), vec![
        vec![Value::Integer(4)], vec![Value::Integer(3)],
        vec![Value::Integer(1)], vec![Value::Integer(2)],
    ]);
}

#[test]
fn composite_sort_expressions_work_before_projection() {
    assert_eq!(rows("SELECT id FROM t ORDER BY COALESCE(score, 0) * -1 ASC, id DESC"), vec![
        vec![Value::Integer(1)], vec![Value::Integer(4)],
        vec![Value::Integer(3)], vec![Value::Integer(2)],
    ]);
}

#[test]
fn aliases_and_ordinals_preserve_correct_sort_values() {
    assert_eq!(rows("SELECT id + 10 AS renamed FROM t ORDER BY renamed DESC LIMIT 2"), vec![
        vec![Value::Integer(14)], vec![Value::Integer(13)],
    ]);
    assert_eq!(rows("SELECT id AS x FROM t ORDER BY 1 DESC LIMIT 2"), vec![
        vec![Value::Integer(4)], vec![Value::Integer(3)],
    ]);
    assert_eq!(rows("SELECT id + 1 AS x FROM t ORDER BY x * -1"), vec![
        vec![Value::Integer(5)], vec![Value::Integer(4)],
        vec![Value::Integer(3)], vec![Value::Integer(2)],
    ]);
}

#[test]
fn numeric_constants_and_computed_expressions_are_not_misparsed_as_ordinals() {
    assert_eq!(rows("SELECT id FROM t ORDER BY 1.5, id DESC LIMIT 1"), vec![vec![Value::Integer(4)]]);
    assert_eq!(rows("SELECT id FROM t ORDER BY 1 + id DESC LIMIT 1"), vec![vec![Value::Integer(4)]]);
    assert!(parse_sql("SELECT id FROM t ORDER BY 0").is_err());
    assert!(parse_sql("SELECT id FROM t ORDER BY 3").is_err());
}

#[test]
fn grouped_and_distinct_sorting_do_not_lose_results() {
    assert_eq!(rows("SELECT score, COUNT(*) AS n FROM t GROUP BY score ORDER BY n DESC LIMIT 1"),
        vec![vec![Value::Integer(10), Value::Integer(2)]]);
    let distinct = rows("SELECT DISTINCT score FROM t ORDER BY score ASC NULLS LAST");
    assert_eq!(distinct.len(), 3);
    assert_eq!(distinct[0], vec![Value::Integer(10)]);
    assert_eq!(distinct[1], vec![Value::Integer(20)]);
    assert_eq!(distinct[2], vec![Value::Null]);
}

#[test]
fn sorting_unknown_source_column_fails_even_on_empty_input() {
    let plan = parse_sql("SELECT id FROM t ORDER BY does_not_exist").unwrap();
    assert!(execute(&plan, &catalog()).is_err());
}
