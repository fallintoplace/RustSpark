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
        vec![Value::Text("a".into()), Value::Integer(7)],
        vec![Value::Text("a".into()), Value::Null],
        vec![Value::Text("a".into()), Value::Integer(3)],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Integer(0)],
    ])?)?;
    execute(&parse_sql(sql)?, &catalog)?.rows()
}

#[test]
fn spark_bit_xor_groups_and_skips_nulls() {
    assert_eq!(run("SELECT g, BIT_XOR(n) AS value FROM data GROUP BY g ORDER BY g").unwrap(), vec![
        vec![Value::Text("a".into()), Value::Integer(4)],
        vec![Value::Text("b".into()), Value::Null],
        vec![Value::Text("c".into()), Value::Integer(0)],
    ]);
}

#[test]
fn spark_bit_xor_filter_having_and_input_types() {
    assert_eq!(run("SELECT BIT_XOR(n) FILTER (WHERE n > 0) AS value FROM data").unwrap(),
        vec![vec![Value::Integer(4)]]);
    assert_eq!(run("SELECT g, BIT_XOR(n) AS value FROM data GROUP BY g HAVING BIT_XOR(n) > 0 ORDER BY g").unwrap(),
        vec![vec![Value::Text("a".into()), Value::Integer(4)]]);
    assert!(run("SELECT BIT_XOR(g) FROM data").is_err());
}
