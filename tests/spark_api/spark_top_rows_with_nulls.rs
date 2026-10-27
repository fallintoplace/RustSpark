use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn top_rows_respects_explicit_null_placement() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "values_table",
            "SELECT CAST(NULL AS BIGINT) AS n UNION ALL SELECT 3 UNION ALL SELECT 1",
        )
        .await?;

    let null_last = session
        .top_rows_with_nulls("values_table", &[("n", true, false)], 2)
        .await?;
    session.register_dataframe("nonnull", null_last)?;
    assert_eq!(session.table("nonnull").await?.count().await?, 2);
    assert_eq!(
        session.table_first_row("nonnull").await?,
        Some(vec![ScalarValue::Int64(Some(1))])
    );

    let null_first = session
        .top_rows_with_nulls("values_table", &[("n", true, true)], 1)
        .await?;
    session.register_dataframe("missing_first", null_first)?;
    assert_eq!(
        session.table_first_row("missing_first").await?,
        Some(vec![ScalarValue::Int64(None)])
    );
    assert_eq!(
        session
            .top_rows_with_nulls("values_table", &[("n", true, false)], 0)
            .await?
            .count()
            .await?,
        0
    );
    assert!(
        session
            .top_rows_with_nulls("values_table", &[], 1)
            .await
            .is_err()
    );
    assert!(
        session
            .top_rows_with_nulls("values_table", &[("unknown", true, false)], 1)
            .await
            .is_err()
    );
    Ok(())
}
