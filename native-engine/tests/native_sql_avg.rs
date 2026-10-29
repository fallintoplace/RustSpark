use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute, scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};

#[test]
fn avg_groups_and_skips_nulls() {
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(
        Schema::new(vec![Field::new("k", DataType::Text, false),Field::new("n", DataType::Integer, true)]).unwrap(),
        vec![
            vec![Value::Text("a".into()),Value::Integer(2)],
            vec![Value::Text("a".into()),Value::Null],
            vec![Value::Text("a".into()),Value::Integer(4)],
            vec![Value::Text("b".into()),Value::Null],
        ],
    ).unwrap()).unwrap();
    let result = execute(&parse_sql("SELECT k, AVG(n) AS mean FROM data GROUP BY k ORDER BY k").unwrap(), &catalog).unwrap();
    assert_eq!(result.rows().unwrap(), vec![
        vec![Value::Text("a".into()),Value::Float(3.0)],
        vec![Value::Text("b".into()),Value::Null],
    ]);
    assert!(execute(&parse_sql("SELECT AVG(k) AS bad FROM data").unwrap(),&catalog).is_err());
}
