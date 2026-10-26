use rustspark::RustSparkSession;

#[tokio::test]
async fn deduplicates_union_of_reordered_sql_columns() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let combined = session
        .union_sql_distinct_by_name(&[
            "SELECT 1 AS id, 2 AS amount",
            "SELECT 2 AS amount, 1 AS id",
            "SELECT 3 AS id, 4 AS amount",
        ])
        .await?;
    assert_eq!(combined.count().await?, 2);
    assert_eq!(combined.schema().fields().len(), 2);
    assert!(session.union_sql_distinct_by_name(&[]).await.is_err());
    Ok(())
}
