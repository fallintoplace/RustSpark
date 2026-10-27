use rustspark::RustSparkSession;

#[tokio::test]
async fn counts_registered_table_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3",
        )
        .await?;
    session
        .register_sql_view("empty", "SELECT 1 AS n WHERE FALSE")
        .await?;
    assert_eq!(session.table_row_count("numbers").await?, 3);
    assert_eq!(session.table_row_count("empty").await?, 0);
    assert!(session.table_row_count("missing").await.is_err());
    Ok(())
}
