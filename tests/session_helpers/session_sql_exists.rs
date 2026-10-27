use rustspark::RustSparkSession;

#[tokio::test]
async fn checks_for_rows_without_executing_writes() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert!(session.sql_exists("SELECT 42").await?);
    assert!(!session.sql_exists("SELECT 42 WHERE FALSE").await?);
    assert!(
        session
            .sql_exists("CREATE TABLE unsafe_table AS SELECT 1")
            .await
            .is_err()
    );
    assert!(!session.table_exists("unsafe_table")?);
    Ok(())
}
