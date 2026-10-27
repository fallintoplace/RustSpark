use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn fetches_one_registered_table_row() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "single",
            "SELECT CAST(7 AS BIGINT) AS value, CAST(NULL AS BIGINT) AS missing",
        )
        .await?;
    session
        .register_sql_view("empty", "SELECT CAST(1 AS BIGINT) AS n WHERE FALSE")
        .await?;

    assert_eq!(
        session.table_first_row("single").await?,
        Some(vec![ScalarValue::Int64(Some(7)), ScalarValue::Int64(None)])
    );
    assert_eq!(session.table_first_row("empty").await?, None);
    assert!(session.table_first_row("unknown").await.is_err());
    Ok(())
}
