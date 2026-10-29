use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, join::cross_join, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql, stages::{StageGraph, StageKind},
};

fn table(name: &str, numbers: &[Option<i64>]) -> Batch {
    Batch::from_rows(
        Schema::new(vec![Field::new(name, DataType::Integer, true)]).unwrap(),
        numbers.iter().map(|number| vec![number.map(Value::Integer).unwrap_or(Value::Null)]).collect(),
    ).unwrap()
}

#[test]
fn cross_join_runs_through_sql_and_produces_full_cartesian_product() {
    let mut catalog = Catalog::new();
    catalog.register("a", table("x", &[Some(1), Some(2)])).unwrap();
    catalog.register("b", table("y", &[Some(10), None])).unwrap();
    let plan = parse_sql("SELECT * FROM a CROSS JOIN b").unwrap();
    let result = execute(&plan, &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Integer(1), Value::Integer(10)],
        vec![Value::Integer(1), Value::Null],
        vec![Value::Integer(2), Value::Integer(10)],
        vec![Value::Integer(2), Value::Null],
    ]);
    let graph = StageGraph::from_plan(&plan).unwrap();
    assert_eq!(graph.stages()[graph.root().unwrap()].kind, StageKind::CrossJoin);
}

#[test]
fn cross_join_supports_where_and_disambiguates_names() {
    let mut catalog = Catalog::new();
    catalog.register("a", table("id", &[Some(1), Some(2)])).unwrap();
    catalog.register("b", table("id", &[Some(2), Some(3)])).unwrap();
    let result = execute(&parse_sql(
        "SELECT id, right_id FROM a CROSS JOIN b WHERE id = right_id",
    ).unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Integer(2), Value::Integer(2)],
    ]);
}

#[test]
fn cross_join_keeps_empty_schema_and_input_nullability() {
    let a = table("a", &[Some(1)]);
    let b = table("b", &[]);
    let result = cross_join(&a, &b).unwrap();
    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.schema().len(), 2);
    assert!(result.schema().field(0).unwrap().nullable);
}

#[test]
fn huge_cartesian_products_are_rejected_before_row_allocation() {
    let a = table("a", &vec![Some(1); 1001]);
    let b = table("b", &vec![Some(2); 1001]);
    assert!(cross_join(&a, &b).unwrap_err().to_string().contains("safety limit"));
}

#[test]
fn cross_join_does_not_swallow_an_on_clause_or_malformed_syntax() {
    assert!(parse_sql("SELECT * FROM a CROSS JOIN b ON a = b").is_err());
    assert!(parse_sql("SELECT * FROM a CROSS b").is_err());
}
