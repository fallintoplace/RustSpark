use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

#[tokio::test]
async fn pages_sorted_sql_and_checks_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")).await?;

    let batches = session.sql_page("SELECT amount FROM sales ORDER BY amount", 1, 1)
        .await?.collect().await?;
    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let amount = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(amount.value(0), 10);

    assert_eq!(session.sql_page("SELECT * FROM sales", 100, 10).await?.count().await?, 0);
    assert!(session.sql_page("SELECT 1", 0, 0).await.is_err());
    assert!(session.sql_page("SELECT 1", usize::MAX, 2).await.is_err());
    Ok(())
}
