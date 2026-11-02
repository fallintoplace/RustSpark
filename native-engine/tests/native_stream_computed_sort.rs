use rustspark_native::{
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, StreamOptions}},
    schema::{DataType, Field, Schema},
    scalar::Value,
    sql::parse_sql, Result,
};
use std::{fs, sync::atomic::{AtomicU64, Ordering}};

static ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn external_sort_evaluates_computed_keys_and_hides_them() {
    let path = std::env::temp_dir().join(format!(
        "rustspark-sort-expr-{}-{}.csv",
        std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
    fs::write(&path, "id,n\n1,9\n2,3\n3,6\n4,3\n").unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false),
        Field::new("n", DataType::Integer, false),
    ]).unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_csv("sales", &path, schema, 2048).unwrap();
    let plan = parse_sql("SELECT id FROM sales ORDER BY n * 2 DESC, id ASC").unwrap();
    let options = StreamOptions {
        batch_rows: 2, sort_run_rows: 2, merge_fan_in: 2,
        ..StreamOptions::default()
    };
    let batches = execute(&plan, &catalog, &options, QueryControl::default())
        .unwrap().collect::<Result<Vec<_>>>().unwrap();
    let rows = batches.iter().flat_map(|batch| {
        assert_eq!(batch.schema().len(), 1, "sort keys must not appear in output");
        batch.rows().unwrap()
    }).collect::<Vec<_>>();
    assert_eq!(rows, vec![
        vec![Value::Integer(1)], vec![Value::Integer(3)],
        vec![Value::Integer(2)], vec![Value::Integer(4)],
    ]);
    fs::remove_file(path).unwrap();
}
