use rustspark::RustSparkSession;

#[tokio::test]
async fn counts_unique_rows_including_one_null_row() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT id FROM (VALUES (1), (1), (2), (NULL), (NULL)) AS t(id)",
        )
        .await?;
    assert_eq!(session.distinct_row_count("items").await?, 3);
    assert_eq!(session.table("items").await?.count().await?, 5);

    session
        .register_sql_view("empty_items", "SELECT 1 AS id WHERE FALSE")
        .await?;
    assert_eq!(session.distinct_row_count("empty_items").await?, 0);
    assert!(session.distinct_row_count("missing").await.is_err());
    Ok(())
}
