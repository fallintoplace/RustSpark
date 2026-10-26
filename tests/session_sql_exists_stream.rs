use rustspark::RustSparkSession;

#[tokio::test]
async fn existence_checks_short_circuit_or_report_empty() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert!(session.sql_exists("SELECT 1 UNION ALL SELECT 2").await?);
    assert!(!session.sql_exists("SELECT 1 WHERE FALSE").await?);
    assert!(session.sql_exists_with_params("SELECT 1 WHERE 5 > $1", vec![
        datafusion::common::ScalarValue::Int64(Some(3))
    ]).await?);
    assert!(!session.sql_exists_with_params("SELECT 1 WHERE 5 > $1", vec![
        datafusion::common::ScalarValue::Int64(Some(8))
    ]).await?);
    Ok(())
}
