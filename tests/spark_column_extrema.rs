use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn extrema_ignores_nulls_and_quotes_column_names() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "metrics",
            "SELECT CAST(NULL AS BIGINT) AS \"odd\"\"metric\"
             UNION ALL SELECT 9 UNION ALL SELECT 5",
        )
        .await?;
    let extrema = session.column_extrema("metrics", "odd\"metric").await?;
    session.register_dataframe("extrema", extrema)?;
    assert_eq!(
        session.sql_scalar("SELECT min_value FROM extrema").await?,
        ScalarValue::Int64(Some(5))
    );
    assert_eq!(
        session.sql_scalar("SELECT max_value FROM extrema").await?,
        ScalarValue::Int64(Some(9))
    );
    assert!(session.column_extrema("metrics", "missing").await.is_err());
    Ok(())
}

#[tokio::test]
async fn extrema_returns_null_for_empty_input() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("empty", "SELECT CAST(1 AS BIGINT) AS n WHERE FALSE")
        .await?;
    let extrema = session.column_extrema("empty", "n").await?;
    session.register_dataframe("result", extrema)?;
    assert_eq!(
        session.sql_scalar("SELECT min_value FROM result").await?,
        ScalarValue::Int64(None)
    );
    Ok(())
}
