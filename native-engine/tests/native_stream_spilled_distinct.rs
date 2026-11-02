use rustspark_native::{
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, StreamOptions}},
    schema::{DataType, Field, Schema},
    scalar::Value,
    sql::parse_sql, Result,
};
use std::{collections::HashSet, fs, sync::atomic::{AtomicU64, Ordering}};

static ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn sql_distinct_reads_spilled_unique_keys_across_batches() {
    let path = std::env::temp_dir().join(format!(
        "rustspark-distinct-stream-{}-{}.csv",
        std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
    fs::write(&path, "id,category\n1,aa\n2,bb\n3,aa\n4,bb\n5,cc\n6,cc\n").unwrap();
    let mut catalog = FileCatalog::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false),
        Field::new("category", DataType::Text, false),
    ]).unwrap();
    catalog.register_csv("events", &path, schema, 1024).unwrap();
    let plan = parse_sql("SELECT DISTINCT category FROM events").unwrap();
    let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
    let batches = execute(&plan, &catalog, &options, QueryControl::default())
        .unwrap().collect::<Result<Vec<_>>>().unwrap();
    let keys = batches.iter().flat_map(|batch| batch.rows().unwrap())
        .map(|row| row[0].clone()).collect::<HashSet<Value>>();
    assert_eq!(keys.len(), 3);
    for key in ["aa", "bb", "cc"] {
        assert!(keys.contains(&Value::Text(key.into())));
    }
    fs::remove_file(path).unwrap();
}
