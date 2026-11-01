use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn query(sql: &str) -> Vec<Vec<Value>> {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Text, false),
        Field::new("n", DataType::Integer, true),
    ]).unwrap();
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("a".into()), Value::Integer(8)],
        vec![Value::Text("b".into()), Value::Null],
    ]).unwrap()).unwrap();
    execute(&parse_sql(sql).unwrap(), &catalog).unwrap().rows().unwrap()
}

#[test]
fn mean_alias_supports_grouping_and_nulls() {
    assert_eq!(query("SELECT k, MEAN(n) AS average FROM data GROUP BY k ORDER BY k"), vec![
        vec![Value::Text("a".into()), Value::Float(4.0)],
        vec![Value::Text("b".into()), Value::Null],
    ]);
}

#[test]
fn mean_alias_supports_distinct_and_having() {
    assert_eq!(query("SELECT k, MEAN(DISTINCT n) AS average FROM data GROUP BY k ORDER BY k"), vec![
        vec![Value::Text("a".into()), Value::Float(5.0)],
        vec![Value::Text("b".into()), Value::Null],
    ]);
    assert_eq!(query("SELECT k, MEAN(n) AS average FROM data GROUP BY k HAVING MEAN(n) > 3 ORDER BY k"),
        vec![vec![Value::Text("a".into()), Value::Float(4.0)]]);
}
