use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn scalar_stream_validates_empty_and_extra_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    assert_eq!(
        session.sql_scalar("SELECT CAST(NULL AS BIGINT)").await?,
        ScalarValue::Int64(None)
    );
    assert_eq!(
        session.sql_scalar("SELECT CAST(99 AS BIGINT)").await?,
        ScalarValue::Int64(Some(99))
    );
    assert!(session.sql_scalar("SELECT 1 UNION ALL SELECT 2").await.is_err());
    assert!(session.sql_scalar("SELECT 1 WHERE FALSE").await.is_err());
    assert!(session.sql_scalar("SELECT 1 AS first, 2 AS second").await.is_err());
    Ok(())
}
