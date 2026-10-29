use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute, scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};

#[test]
fn count_distinct_groups_and_ignores_nulls() {
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(
        Schema::new(vec![Field::new("g", DataType::Text, false),Field::new("n", DataType::Integer, true)]).unwrap(),
        vec![
            vec![Value::Text("a".into()),Value::Integer(2)],
            vec![Value::Text("a".into()),Value::Integer(2)],
            vec![Value::Text("a".into()),Value::Null],
            vec![Value::Text("a".into()),Value::Integer(3)],
            vec![Value::Text("b".into()),Value::Null],
        ],
    ).unwrap()).unwrap();
    let result = execute(&parse_sql("SELECT g, COUNT(DISTINCT n) AS uniques FROM data GROUP BY g ORDER BY g").unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Text("a".into()),Value::Integer(2)],
        vec![Value::Text("b".into()),Value::Integer(0)],
    ]);
}
