use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(sql: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let schema = Schema::new(vec![
        Field::new("g", DataType::Text, false),
        Field::new("n", DataType::Integer, true),
    ])?;
    let mut catalog = Catalog::new();
    catalog.register("data", Batch::from_rows(schema, vec![
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("a".into()), Value::Integer(3)],
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Integer(4)],
        vec![Value::Text("c".into()), Value::Null],
    ])?)?;
    execute(&parse_sql(sql)?, &catalog)?.rows()
}

#[test]
fn spark_first_respects_row_order_and_nulls() {
    assert_eq!(run("SELECT g, FIRST(n) AS value FROM data GROUP BY g ORDER BY g").unwrap(), vec![
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Integer(4)],
    ]);
    assert_eq!(run("SELECT g, FIRST(n, TRUE) AS value FROM data GROUP BY g ORDER BY g").unwrap(), vec![
        vec![Value::Text("a".into()), Value::Integer(2)],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Integer(4)],
    ]);
}

#[test]
fn spark_first_supports_having_with_ignore_nulls() {
    assert_eq!(run("SELECT g, FIRST(n, TRUE) AS value FROM data GROUP BY g HAVING FIRST(n, TRUE) > 2 ORDER BY g").unwrap(),
        vec![vec![Value::Text("c".into()), Value::Integer(4)]]);
    assert!(run("SELECT FIRST(n, 1) FROM data").is_err());
    assert!(run("SELECT FIRST(n) FILTER (WHERE n > 0) FROM data").is_err());
}
