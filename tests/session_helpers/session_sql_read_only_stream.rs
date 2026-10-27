use datafusion::arrow::array::Int64Array;
use futures::StreamExt;
use rustspark::RustSparkSession;

#[tokio::test]
async fn streams_safe_sql_and_rejects_mutations() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let mut stream = session
        .sql_read_only_stream("SELECT CAST(9 AS BIGINT) AS value")
        .await?;
    let batch = stream.next().await.ok_or("missing batch")??;
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("expected Int64")?;
    assert_eq!(column.value(0), 9);
    assert!(
        session
            .sql_read_only_stream("CREATE TABLE denied AS SELECT 1")
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}
