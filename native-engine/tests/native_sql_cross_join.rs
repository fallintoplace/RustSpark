use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, join, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let first = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
    let second = Schema::new(vec![
        Field::new("id", DataType::Integer, true),
        Field::new("label", DataType::Text, false),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("a", Batch::from_rows(first, vec![
        vec![Value::Integer(1)], vec![Value::Null],
    ]).unwrap()).unwrap();
    catalog.register("b", Batch::from_rows(second, vec![
        vec![Value::Integer(1), Value::Text("a".into())],
        vec![Value::Integer(2), Value::Text("b".into())],
    ]).unwrap()).unwrap();
    catalog
}

#[test]
fn cross_join_keeps_cartesian_multiplicity_and_column_types() {
    let expected = vec![
        vec![Value::Integer(1), Value::Integer(1), Value::Text("a".into())],
        vec![Value::Integer(1), Value::Integer(2), Value::Text("b".into())],
        vec![Value::Null, Value::Integer(1), Value::Text("a".into())],
        vec![Value::Null, Value::Integer(2), Value::Text("b".into())],
    ];
    for query in ["SELECT * FROM a CROSS JOIN b", "SELECT * FROM a, b"] {
        let result = execute(&parse_sql(query).unwrap(), &catalog()).unwrap();
        assert_eq!(result.rows().unwrap(), expected);
        assert_eq!(result.schema().column("right_id").unwrap(), 1);
        assert_eq!(result.schema().field(2).unwrap().nullable, false);
    }
    let rows = execute(&parse_sql(
        "SELECT id, right_id FROM a CROSS JOIN b WHERE id = right_id"
    ).unwrap(), &catalog()).unwrap().rows().unwrap();
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(1)]]);
}

#[test]
fn cartesian_join_empty_inputs_and_output_budget_are_checked() {
    let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
    let empty = Batch::empty(schema.clone());
    let large = Batch::from_rows(schema, (0..1001)
        .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
    let empty_result = join::cross_join(&empty, &large).unwrap();
    assert_eq!(empty_result.num_rows(), 0);
    assert_eq!(empty_result.schema().len(), 2);
    let error = join::cross_join(&large, &large).unwrap_err();
    assert!(error.to_string().contains("safety limit"));
}

#[test]
fn invalid_cross_join_syntax_is_rejected() {
    assert!(parse_sql("SELECT * FROM a CROSS b").is_err());
    assert!(parse_sql("SELECT * FROM a CROSS JOIN").is_err());
    assert!(parse_sql("SELECT * FROM a CROSS JOIN b ON id = id").is_err());
}
