use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn scalar_queries_return_one_typed_value() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert_eq!(
        session.sql_scalar("SELECT CAST(42 AS BIGINT)").await?,
        ScalarValue::Int64(Some(42))
    );
    assert_eq!(
        session.sql_scalar("SELECT CAST(NULL AS BIGINT)").await?,
        ScalarValue::Int64(None)
    );
    assert!(session.sql_scalar("SELECT 1, 2").await.is_err());
    assert!(session.sql_scalar("SELECT 1 WHERE false").await.is_err());
    assert!(
        session
            .sql_scalar("SELECT 1 UNION ALL SELECT 2")
            .await
            .is_err()
    );
    Ok(())
}
