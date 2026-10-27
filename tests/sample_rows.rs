use rustspark::RustSparkSession;

#[tokio::test]
async fn sample_rows_validates_fractions_and_preserves_source()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4",
        )
        .await?;
    assert_eq!(session.sample_rows("events", 0.0).await?.count().await?, 0);
    assert_eq!(session.sample_rows("events", 1.0).await?.count().await?, 4);
    assert!(session.sample_rows("events", 0.5).await?.count().await? <= 4);
    for invalid in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
        assert!(session.sample_rows("events", invalid).await.is_err());
    }
    assert!(session.sample_rows("unknown", 1.0).await.is_err());
    assert_eq!(session.table_row_count("events").await?, 4);
    Ok(())
}
