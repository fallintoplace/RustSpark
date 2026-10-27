use rustspark::RustSparkSession;

#[tokio::test]
async fn random_row_limit_is_bounded_without_modifying_registered_table()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
        )
        .await?;
    assert_eq!(
        session.take_random_rows("events", 0).await?.count().await?,
        0
    );
    assert_eq!(
        session.take_random_rows("events", 1).await?.count().await?,
        1
    );
    assert_eq!(
        session
            .take_random_rows("events", 10)
            .await?
            .count()
            .await?,
        3
    );
    assert!(session.take_random_rows("missing", 2).await.is_err());
    assert_eq!(session.table_row_count("events").await?, 3);
    Ok(())
}
