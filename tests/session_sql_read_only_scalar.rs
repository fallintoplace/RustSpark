use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn reads_one_scalar_without_allowing_mutation() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert_eq!(
        session
            .sql_read_only_scalar("SELECT CAST(11 AS BIGINT)")
            .await?,
        ScalarValue::Int64(Some(11))
    );
    assert_eq!(
        session
            .sql_read_only_scalar("SELECT CAST(NULL AS BIGINT)")
            .await?,
        ScalarValue::Int64(None)
    );
    assert!(session.sql_read_only_scalar("SELECT 1, 2").await.is_err());
    assert!(
        session
            .sql_read_only_scalar("SELECT 1 WHERE FALSE")
            .await
            .is_err()
    );
    assert!(
        session
            .sql_read_only_scalar("CREATE TABLE denied AS SELECT 1")
            .await
            .is_err()
    );
    assert!(!session.table_exists("denied")?);
    Ok(())
}
