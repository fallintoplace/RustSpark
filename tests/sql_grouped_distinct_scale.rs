use std::sync::Arc;

use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn grouped_distinct_counts_are_exact_with_many_keys() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("bucket", DataType::Utf8, false),
    ]));
    let ids = Int64Array::from((0..10_000).map(i64::from).collect::<Vec<_>>());
    let buckets =
        StringArray::from_iter_values((0..10_000).map(|index| format!("g{:04}", index % 1_000)));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(buckets)])?;

    let session = RustSparkSession::new();
    session.register_batches("events", [batch])?;
    let result = session
        .sql(
            "SELECT bucket, COUNT(*) AS n, COUNT(DISTINCT id) AS unique_ids
             FROM events GROUP BY bucket ORDER BY bucket",
        )
        .await?
        .collect()
        .await?;

    let mut group_count = 0;
    for batch in &result {
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let distinct = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            assert_eq!(counts.value(index), 10);
            assert_eq!(distinct.value(index), 10);
            group_count += 1;
        }
    }
    assert_eq!(group_count, 1_000);
    Ok(())
}
