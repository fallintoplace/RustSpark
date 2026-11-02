use rustspark_native::{
    scale::{
        cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, StreamOptions},
    },
    scalar::Value,
    schema::{DataType, Field, Schema},
    sql::parse_sql,
    Result,
};
use std::{fs, sync::atomic::{AtomicU64, Ordering}};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn computed_group_keys_are_spilled_without_losing_original_aggregate_columns() {
    let path = std::env::temp_dir().join(format!(
        "rustspark-computed-groups-{}-{}.csv",
        std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, "id,n\n1,2\n2,2\n3,3\n4,4\n").unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false),
        Field::new("n", DataType::Integer, false),
    ]).unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_csv("sales", &path, schema, 1024).unwrap();
    let plan = parse_sql(
        "SELECT n + 1 AS bucket, COUNT(*) AS hits, SUM(id) AS total \
         FROM sales GROUP BY n + 1 ORDER BY bucket"
    ).unwrap();
    let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
    let result = execute(&plan, &catalog, &options, QueryControl::default())
        .unwrap().collect::<Result<Vec<_>>>().unwrap();
    let rows = result.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
    assert_eq!(rows, vec![
        vec![Value::Integer(3), Value::Integer(2), Value::Integer(3)],
        vec![Value::Integer(4), Value::Integer(1), Value::Integer(3)],
        vec![Value::Integer(5), Value::Integer(1), Value::Integer(4)],
    ]);
    fs::remove_file(path).unwrap();
}

#[test]
fn computed_group_key_type_errors_are_rejected_before_reading() {
    let catalog = FileCatalog::new();
    let plan = parse_sql("SELECT unknown + 1 FROM missing GROUP BY unknown + 1").unwrap();
    assert!(execute(&plan, &catalog, &StreamOptions::default(),
        QueryControl::default()).is_err());
}
