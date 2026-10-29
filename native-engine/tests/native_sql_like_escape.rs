use rustspark_native::{
    batch::Batch, catalog::Catalog, executor::execute, scalar::Value,
    schema::{DataType, Field, Schema}, sql::parse_sql,
};

fn run(query: &str) -> rustspark_native::Result<Vec<Vec<Value>>> {
    let schema = Schema::new(vec![Field::new("text", DataType::Text, true)])?;
    let mut catalog = Catalog::new();
    catalog.register("items", Batch::from_rows(schema, vec![
        vec![Value::Text("100% done".into())],
        vec![Value::Text("1000 done".into())],
        vec![Value::Text("a_b".into())],
        vec![Value::Text("axb".into())],
        vec![Value::Text("café".into())],
        vec![Value::Null],
    ])?)?;
    execute(&parse_sql(query)?, &catalog)?.rows()
}

#[test]
fn spark_like_default_escape_matches_literal_percent_and_underscore() {
    assert_eq!(run("SELECT text FROM items WHERE text LIKE '100\\% done'").unwrap(),
        vec![vec![Value::Text("100% done".into())]]);
    assert_eq!(run("SELECT text FROM items WHERE text LIKE 'a\\_b'").unwrap(),
        vec![vec![Value::Text("a_b".into())]]);
    assert_eq!(run("SELECT text FROM items WHERE text LIKE 'caf_'").unwrap(),
        vec![vec![Value::Text("café".into())]]);
}

#[test]
fn spark_like_custom_escape_works_with_negation_and_case_insensitivity() {
    assert_eq!(run("SELECT text FROM items WHERE text LIKE '100$% done' ESCAPE '$'").unwrap(),
        vec![vec![Value::Text("100% done".into())]]);
    assert_eq!(run("SELECT text FROM items WHERE text ILIKE 'A$_B' ESCAPE '$'").unwrap(),
        vec![vec![Value::Text("a_b".into())]]);
    assert_eq!(run("SELECT text FROM items WHERE text NOT LIKE '%$%%' ESCAPE '$'").unwrap(),
        vec![
            vec![Value::Text("1000 done".into())],
            vec![Value::Text("a_b".into())],
            vec![Value::Text("axb".into())],
            vec![Value::Text("café".into())],
        ]);
}

#[test]
fn invalid_escapes_fail_instead_of_silently_changing_matches() {
    assert!(run("SELECT text FROM items WHERE text LIKE 'x\\q'").is_err());
    assert!(run("SELECT text FROM items WHERE text LIKE 'x\\'").is_err());
    assert!(parse_sql("SELECT text FROM items WHERE text LIKE 'a%' ESCAPE 'long'").is_err());
    assert!(parse_sql("SELECT text FROM items WHERE text LIKE 'a%' ESCAPE ''").is_err());
}
