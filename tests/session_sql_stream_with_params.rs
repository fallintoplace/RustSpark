use datafusion::{arrow::array::Int64Array, common::ScalarValue};
use futures::StreamExt;
use rustspark::RustSparkSession;

#[tokio::test]
async fn streams_bound_queries_without_collecting() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let mut stream = session
        .sql_stream_with_params(
            "SELECT CAST($1 AS BIGINT) AS value",
            vec![ScalarValue::Int64(Some(37))],
        )
        .await?;
    let batch = stream.next().await.ok_or("missing result batch")??;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("expected Int64")?;
    assert_eq!(values.value(0), 37);
    assert!(stream.next().await.is_none());

    assert!(
        session
            .sql_stream_with_params("SELECT $1", vec![ScalarValue::Int64(Some(1))])
            .await
            .is_ok()
    );
    Ok(())
}
