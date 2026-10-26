use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn first_row_stream_returns_typed_values_only() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let row = session.sql_first_row(
        "SELECT CAST(17 AS BIGINT) AS id, CAST(NULL AS BIGINT) AS missing"
    ).await?;
    assert_eq!(row, Some(vec![
        ScalarValue::Int64(Some(17)),
        ScalarValue::Int64(None),
    ]));
    assert_eq!(session.sql_first_row("SELECT 1 WHERE FALSE").await?, None);
    assert!(session.sql_first_row("SELECT 1, 2 FROM missing").await.is_err());
    Ok(())
}
