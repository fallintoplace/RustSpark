use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};
fn query(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![
        Field::new("g", DataType::Text, false),
        Field::new("n", DataType::Integer, true),
    ])?;
    catalog.register("t", Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("a".into()), Value::Integer(5)],
        vec![Value::Text("b".into()), Value::Null],
    ])?)?;
    execute(&parse_sql(sql)?, &catalog)?.rows()
}
#[test]
fn any_value_uses_first_value_and_optional_ignore_nulls() {
    assert_eq!(query("SELECT g, ANY_VALUE(n) AS v FROM t GROUP BY g ORDER BY g").unwrap(),
        vec![vec![Value::Text("a".into()), Value::Null],
             vec![Value::Text("b".into()), Value::Null]]);
    assert_eq!(query("SELECT g, ANY_VALUE(n, TRUE) AS v FROM t GROUP BY g ORDER BY g").unwrap(),
        vec![vec![Value::Text("a".into()), Value::Integer(5)],
             vec![Value::Text("b".into()), Value::Null]]);
    assert_eq!(query("SELECT g FROM t GROUP BY g HAVING ANY_VALUE(n, TRUE) > 3 ORDER BY g").unwrap(),
        vec![vec![Value::Text("a".into())]]);
    assert!(query("SELECT ANY_VALUE(n, n > 0) FROM t").is_err());
}
