use futures::StreamExt;
use rustspark::RustSparkSession;

#[tokio::test]
async fn streams_sql_batches_without_collecting_results()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    let mut stream = session.sql_stream("SELECT amount FROM sales").await?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 3);
    assert!(session.sql_stream("SELECT * FROM missing").await.is_err());
    Ok(())
}
