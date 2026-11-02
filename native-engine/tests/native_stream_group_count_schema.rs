use rustspark_native::{
    aggregate::{AggregateItem, AggregateOp}, expr::Expr, plan::Plan,
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, output_schema, StreamOptions}},
    scalar::Value, schema::{DataType, Field, Schema},
    sort::SortKey, Result,
};
use std::{fs, sync::atomic::{AtomicU64, Ordering}};
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn grouped_count_schema_survives_external_sort_and_topk() {
    let dir = std::env::temp_dir().join(format!("rustspark-count-schema-{}-{}",
        std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed)));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rows.csv");
    fs::write(&path, "region,n\na,1\nb,2\na,3\na,4\n").unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_csv("events", &path, Schema::new(vec![
        Field::new("region", DataType::Text, false),
        Field::new("n", DataType::Integer, false),
    ]).unwrap(), 1024).unwrap();
    let aggregate = Plan::Aggregate {
        input: Box::new(Plan::scan("events")),
        group_by: vec![(Expr::Column("region".into()), "region".into())],
        aggregates: vec![AggregateItem::new(AggregateOp::CountAll, "n")],
    };
    let sorted = Plan::Sort {
        input: Box::new(aggregate),
        keys: vec![SortKey {
            expression: Expr::Column("n".into()),
            descending: true, nulls_first: false,
        }],
    };
    let options = StreamOptions {
        batch_rows: 2, sort_run_rows: 2, merge_fan_in: 2,
        scratch_directory: dir.clone(), ..StreamOptions::default()
    };
    for plan in [
        sorted.clone(),
        Plan::Limit { input: Box::new(sorted), offset: 0, count: Some(1) },
    ] {
        let schema = output_schema(&plan, &catalog, &options).unwrap();
        assert!(!schema.field(1).unwrap().nullable);
        let batches = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = batches.iter().flat_map(|batch| {
            assert_eq!(batch.schema(), &schema);
            batch.rows().unwrap()
        }).collect::<Vec<_>>();
        assert_eq!(rows.first(), Some(&vec![
            Value::Text("a".into()), Value::Integer(3)]));
        assert_eq!(rows.len(), if matches!(plan, Plan::Limit { .. }) { 1 } else { 2 });
    }
    fs::remove_dir_all(dir).unwrap();
}
