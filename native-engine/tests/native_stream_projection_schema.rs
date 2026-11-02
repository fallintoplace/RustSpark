use rustspark_native::{
    scale::{cancel::QueryControl, file_catalog::FileCatalog,
        stream_execute::{execute, output_schema, StreamOptions}},
    expr::Expr, plan::Plan, project::Selection, sort::SortKey,
    scalar::Value, schema::{DataType, Field, Schema}, sql::parse_sql, Result,
};
use std::{fs, sync::atomic::{AtomicU64, Ordering}};
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn non_nullable_projections_can_be_sorted_and_topk_selected() {
    let dir = std::env::temp_dir().join(format!("rustspark-project-schema-{}-{}",
        std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed)));
    fs::create_dir_all(&dir).unwrap();
    let input = dir.join("input.csv");
    fs::write(&input, "id,n\n3,9\n1,4\n2,7\n").unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_csv("events", &input, Schema::new(vec![
        Field::new("id", DataType::Integer, false),
        Field::new("n", DataType::Integer, false),
    ]).unwrap(), 1024).unwrap();
    let options = StreamOptions {
        batch_rows: 2, sort_run_rows: 2, merge_fan_in: 2,
        scratch_directory: dir.clone(), ..StreamOptions::default()
    };
    for (sql, expected) in [
        ("SELECT id FROM events ORDER BY id DESC", vec![3, 2, 1]),
        ("SELECT id FROM events ORDER BY id DESC LIMIT 2", vec![3, 2]),
    ] {
        let plan = parse_sql(sql).unwrap();
        let schema = output_schema(&plan, &catalog, &options).unwrap();
        let batches = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = batches.iter().flat_map(|batch| {
            assert_eq!(batch.schema(), &schema);
            batch.rows().unwrap()
        }).collect::<Vec<_>>();
        assert_eq!(rows, expected.into_iter()
            .map(|value| vec![Value::Integer(value)]).collect::<Vec<_>>());
    }
    // SQL commonly places ORDER BY before final projection. Exercise the
    // other physical shape directly: sorting a non-nullable projected batch.
    let projected = Plan::Project {
        input: Box::new(Plan::scan("events")),
        selections: vec![Selection::new(Expr::Column("id".into()), "id")],
    };
    let projected_schema = output_schema(&projected, &catalog, &options).unwrap();
    assert!(!projected_schema.field(0).unwrap().nullable);
    let order = SortKey {
        expression: Expr::Column("id".into()),
        descending: true, nulls_first: false,
    };
    let sorted = Plan::Sort {
        input: Box::new(projected),
        keys: vec![order],
    };
    for (plan, expected) in [
        (sorted.clone(), vec![3, 2, 1]),
        (Plan::Limit { input: Box::new(sorted), offset: 0, count: Some(2) }, vec![3, 2]),
    ] {
        let schema = output_schema(&plan, &catalog, &options).unwrap();
        assert!(!schema.field(0).unwrap().nullable);
        let result = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = result.iter().flat_map(|batch| {
            assert_eq!(batch.schema(), &schema);
            batch.rows().unwrap()
        }).collect::<Vec<_>>();
        assert_eq!(rows, expected.into_iter()
            .map(|value| vec![Value::Integer(value)]).collect::<Vec<_>>());
    }
    fs::remove_dir_all(dir).unwrap();
}
