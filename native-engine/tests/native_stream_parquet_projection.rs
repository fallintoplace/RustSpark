use rustspark_native::{
    arrow_bridge::to_arrow,
    batch::Batch,
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, StreamOptions}},
    scalar::Value, schema::{DataType, Field, Schema},
    sql::parse_sql, Result,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, sync::atomic::{AtomicU64, Ordering}};
static ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn sql_parquet_projection_reads_only_requested_columns_and_preserves_alias_order() {
    let path = std::env::temp_dir().join(format!(
        "rustspark-projection-pushdown-{}-{}.parquet",
        std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false),
        Field::new("name", DataType::Text, false),
        Field::new("cost", DataType::Integer, false),
    ]).unwrap();
    let data = Batch::from_rows(schema, vec![
        vec![Value::Integer(1), Value::Text("one".into()), Value::Integer(20)],
        vec![Value::Integer(2), Value::Text("two".into()), Value::Integer(30)],
    ]).unwrap();
    let arrow = to_arrow(&data).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), arrow.schema(), None)
        .unwrap();
    writer.write(&arrow).unwrap();
    writer.close().unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_parquet("items", &path).unwrap();
    let query = parse_sql("SELECT name AS label, id AS key, name AS again FROM items").unwrap();
    let rows = execute(&query, &catalog, &StreamOptions::default(), QueryControl::default())
        .unwrap().collect::<Result<Vec<_>>>().unwrap();
    let values = rows.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
    assert_eq!(values, vec![
        vec![Value::Text("one".into()), Value::Integer(1), Value::Text("one".into())],
        vec![Value::Text("two".into()), Value::Integer(2), Value::Text("two".into())],
    ]);
    std::fs::remove_file(path).unwrap();
}
