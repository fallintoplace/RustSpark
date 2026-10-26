use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn first_row_is_typed_and_empty_results_are_none() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let row = session
        .sql_first_row("SELECT CAST(7 AS BIGINT), CAST('Berlin' AS VARCHAR)")
        .await?;
    assert_eq!(
        row,
        Some(vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Utf8(Some("Berlin".into()))
        ])
    );
    assert_eq!(
        session
            .sql_first_row("SELECT 7 WHERE false")
            .await?,
        None
    );
    assert!(session.sql_first_row("SELECT * FROM missing_table").await.is_err());
    Ok(())
}
