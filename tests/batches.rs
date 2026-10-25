use std::sync::Arc;

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    prelude::{col, lit},
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
async fn reads_multiple_arrow_batches_as_one_lazy_dataframe() -> TestResult {
    let session = RustSparkSession::new();
    let frame = session.read_batches([batch(vec![10, 20]), batch(vec![5])])?;
    assert_eq!(frame.filter(col("amount").gt(lit(9)))?.count().await?, 2);
    assert!(session.sql("SELECT * FROM amount").await.is_err());
    Ok(())
}

#[tokio::test]
async fn keeps_arrow_batch_dataframes_lazy_and_session_local() -> TestResult {
    let session = RustSparkSession::new();
    let frame = session.read_batches([batch(vec![1, 2, 3])])?;
    assert_eq!(frame.count().await?, 3);
    assert!(session.sql("SELECT * FROM data").await.is_err());
    Ok(())
}
