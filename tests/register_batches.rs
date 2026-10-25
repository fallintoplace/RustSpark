use std::sync::Arc;

use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

#[tokio::test]
async fn registers_arrow_batches_for_sql() -> TestResult {
    let session = RustSparkSession::new();
    session.register_batches("events", [batch(vec![10, 20]), batch(vec![5])])?;
    let batches = session
        .sql("SELECT SUM(amount) FROM events")
        .await?
        .collect()
        .await?;
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 35);
    Ok(())
}

#[tokio::test]
async fn rejects_duplicate_arrow_table_names() -> TestResult {
    let session = RustSparkSession::new();
    session.register_batches("events", [batch(vec![1])])?;
    assert!(
        session
            .register_batches("events", [batch(vec![5])])
            .is_err()
    );
    let count = session.sql("SELECT * FROM events").await?.count().await?;
    assert_eq!(count, 1);
    Ok(())
}
