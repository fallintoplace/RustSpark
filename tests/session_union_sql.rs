use rustspark::RustSparkSession;

#[tokio::test]
async fn unions_sql_by_position_and_keeps_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let combined = session
        .union_sql(&[
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(2 AS BIGINT) AS value",
        ])
        .await?;
    assert_eq!(combined.count().await?, 3);
    assert!(session.union_sql(&[]).await.is_err());
    assert!(
        session
            .union_sql(&["SELECT 1 AS a", "SELECT 1 AS a, 2 AS b"])
            .await
            .is_err()
    );
    Ok(())
}
