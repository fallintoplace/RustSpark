use rustspark::RustSparkSession;

#[tokio::test]
async fn column_count_reads_schema_without_scanning_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view("items", "SELECT 1 AS id, 'north' AS region, 9 AS score")
        .await?;
    assert_eq!(session.table_column_count("items").await?, 3);
    assert!(session.table_column_count("missing").await.is_err());
    assert_eq!(session.table_row_count("items").await?, 1);
    Ok(())
}
