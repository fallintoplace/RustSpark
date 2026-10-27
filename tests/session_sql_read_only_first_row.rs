use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn reads_the_first_row_from_read_only_sql() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert_eq!(
        session
            .sql_read_only_first_row("SELECT CAST(5 AS BIGINT), CAST(NULL AS BIGINT)")
            .await?,
        Some(vec![ScalarValue::Int64(Some(5)), ScalarValue::Int64(None)])
    );
    assert_eq!(
        session
            .sql_read_only_first_row("SELECT 1 WHERE FALSE")
            .await?,
        None
    );
    assert!(
        session
            .sql_read_only_first_row("CREATE TABLE denied AS SELECT 1")
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}
