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
fn composed_and_incomplete_existence_predicates_reject() {
    assert!(parse_sql(
        "SELECT id FROM source WHERE EXISTS (SELECT 1) AND id > 1"
    ).is_err());
    assert!(parse_sql(
        "SELECT id FROM source WHERE EXISTS (SELECT 1"
    ).is_err());
}

#[test]
fn existence_queries_are_consistent_in_streaming_sql() {
    use rustspark_native::scale::{
        cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute as stream_execute, StreamOptions},
    };
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    let path = std::env::temp_dir().join(format!(
        "rustspark-exists-{}-{}.csv",
        std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
    fs::write(&path, "id\n1\n2\n1\n").unwrap();
    let mut files = FileCatalog::new();
    let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
    files.register_csv("events", &path, schema, 1024).unwrap();
    let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
    for query in [
        "SELECT id FROM events WHERE EXISTS (SELECT id FROM events WHERE id = 2) ORDER BY id",
        "SELECT id FROM events WHERE NOT EXISTS (SELECT id FROM events WHERE id = 99) ORDER BY id",
    ] {
        let plan = parse_sql(query).unwrap();
        let batches = stream_execute(
            &plan, &files, &options, QueryControl::default()
        ).unwrap().collect::<rustspark_native::Result<Vec<_>>>().unwrap();
        let rows = batches.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)],
        ]);
    }
    fs::remove_file(path).unwrap();
}
