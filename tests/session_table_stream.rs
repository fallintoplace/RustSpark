use futures::StreamExt;
use rustspark::RustSparkSession;

#[tokio::test]
async fn streams_registered_table_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view("numbers", "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await?;
    let mut stream = session.table_stream("numbers").await?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 3);
    assert!(session.table_stream("missing").await.is_err());
    Ok(())
}
