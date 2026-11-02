use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false),
    ]).unwrap();
    catalog.register("source", Batch::from_rows(schema.clone(), vec![
        vec![Value::Integer(1)], vec![Value::Integer(1)],
        vec![Value::Integer(3)],
    ]).unwrap()).unwrap();
    catalog.register("empty", Batch::empty(schema)).unwrap();
    catalog
}
fn rows(query: &str) -> Vec<Vec<Value>> {
    let plan = parse_sql(query).unwrap();
    execute(&plan, &catalog()).unwrap().rows().unwrap()
}

#[test]
fn exists_keeps_each_outer_row_once_even_with_many_inner_matches() {
    assert_eq!(rows(
        "SELECT id FROM source WHERE EXISTS (SELECT id FROM source) ORDER BY id"
    ), vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    assert!(rows(
        "SELECT id FROM source WHERE EXISTS (SELECT id FROM empty)"
    ).is_empty());
}

#[test]
fn not_exists_retains_outer_duplicates_only_when_inner_is_empty() {
    assert_eq!(rows(
        "SELECT id FROM source WHERE NOT EXISTS (SELECT id FROM empty) ORDER BY id"
    ), vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    assert!(rows(
        "SELECT id FROM source WHERE NOT EXISTS (SELECT id FROM source)"
    ).is_empty());
}

#[test]
fn composed_and_correlated_forms_cannot_silently_be_treated_as_exists() {
    assert!(parse_sql(
        "SELECT id FROM source WHERE EXISTS (SELECT 1) AND id > 1"
    ).is_err());
    assert!(parse_sql(
        "SELECT id FROM source WHERE EXISTS (SELECT 1"
    ).is_err());
}
