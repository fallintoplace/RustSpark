use rustspark_native::{
    aggregate::{AggregateItem, AggregateOp},
    batch::Batch, expr::Expr, scalar::Value,
    schema::{DataType, Field, Schema},
    scale::{cancel::QueryControl, grouped_spill::grouped_aggregate, spill::SpillReader},
};
use std::{fs::{self, File}, io::BufReader, sync::atomic::{AtomicU64, Ordering}};

static ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn spark_distinct_numeric_aggregates_spill_across_input_batches() {
    let dir = std::env::temp_dir().join(format!(
        "rustspark-distinct-spill-{}-{}", std::process::id(), ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&dir).unwrap();
    let schema = Schema::new(vec![
        Field::new("g", DataType::Integer, false),
        Field::new("n", DataType::Integer, true),
    ]).unwrap();
    let first = Batch::from_rows(schema.clone(), vec![
        vec![Value::Integer(1), Value::Integer(2)],
        vec![Value::Integer(1), Value::Integer(2)],
        vec![Value::Integer(2), Value::Null],
    ]).unwrap();
    let second = Batch::from_rows(schema.clone(), vec![
        vec![Value::Integer(1), Value::Integer(6)],
        vec![Value::Integer(2), Value::Null],
        vec![Value::Integer(2), Value::Integer(-4)],
        vec![Value::Integer(2), Value::Integer(-4)],
    ]).unwrap();
    let results = grouped_aggregate(
        [Ok(first), Ok(second)], schema,
        &[("g".into(), "g".into())], &[
            AggregateItem::new(AggregateOp::SumDistinct(Expr::Column("n".into())), "total"),
            AggregateItem::new(AggregateOp::AvgDistinct(Expr::Column("n".into())), "mean"),
        ], &dir, 4, 100, 2, &QueryControl::default()
    ).unwrap();
    let mut rows = Vec::new();
    for path in results.paths() {
        let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
        while let Some(batch) = reader.next_batch().unwrap() { rows.extend(batch.rows().unwrap()); }
    }
    assert!(rows.contains(&vec![Value::Integer(1), Value::Integer(8), Value::Float(4.0)]));
    assert!(rows.contains(&vec![Value::Integer(2), Value::Integer(-4), Value::Float(-4.0)]));
    drop(results);
    fs::remove_dir_all(dir).unwrap();
}
