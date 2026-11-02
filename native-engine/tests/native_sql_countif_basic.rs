use rustspark_native::{batch::Batch, catalog::Catalog, executor::execute,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql};
fn query(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let mut catalog = Catalog::new();
    let schema = Schema::new(vec![
        Field::new("g", DataType::Text, false),
        Field::new("n", DataType::Integer, true),
    ])?;
    catalog.register("t", Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("a".into()), Value::Integer(-1)],
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("b".into()), Value::Null],
    ])?)?;
    execute(&parse_sql(sql)?, &catalog)?.rows()
}
#[test]
fn countif_alias_counts_true_only_and_supports_having() {
    let expected = vec![vec![Value::Text("a".into()), Value::Integer(1)],
                        vec![Value::Text("b".into()), Value::Integer(0)]];
    assert_eq!(query("SELECT g, COUNTIF(n > 0) AS c FROM t GROUP BY g ORDER BY g").unwrap(), expected);
    assert_eq!(query("SELECT g, COUNT_IF(n > 0) AS c FROM t GROUP BY g ORDER BY g").unwrap(), expected);
    assert_eq!(query("SELECT g FROM t GROUP BY g HAVING COUNTIF(n > 0) > 0 ORDER BY g").unwrap(),
        vec![vec![Value::Text("a".into())]]);
    assert!(query("SELECT COUNTIF(g) FROM t").is_err());
}
