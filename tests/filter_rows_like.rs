use rustspark::RustSparkSession;

#[tokio::test]
async fn like_uses_typed_patterns_and_sql_null_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "labels",
            "SELECT 'Alpha' AS label UNION ALL SELECT 'alpine' UNION ALL SELECT 'A''quoted' UNION ALL SELECT NULL",
        )
        .await?;
    assert_eq!(session.filter_rows_like("labels", "label", "A%").await?.count().await?, 2);
    assert_eq!(session.filter_rows_like("labels", "label", "A'%").await?.count().await?, 1);
    assert_eq!(
        session.filter_rows_like("labels", "label", "%' OR TRUE --").await?.count().await?,
        0
    );
    assert!(session.filter_rows_like("labels", "missing", "%").await.is_err());
    assert_eq!(session.table_row_count("labels").await?, 4);
    Ok(())
}
