use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn outside_range_excludes_endpoints_and_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "numbers",
            "SELECT CAST(5 AS BIGINT) AS n UNION ALL SELECT 10 UNION ALL SELECT 15
             UNION ALL SELECT 20 UNION ALL SELECT 25 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;

    let outside = session
        .filter_rows_outside_range(
            "numbers",
            "n",
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(20)),
        )
        .await?;
    assert_eq!(outside.count().await?, 2);

    assert!(
        session
            .filter_rows_outside_range(
                "numbers",
                "n",
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(20)),
            )
            .await
            .is_err()
    );
    assert!(
        session
            .filter_rows_outside_range(
                "numbers",
                "missing",
                ScalarValue::Int64(Some(10)),
                ScalarValue::Int64(Some(20)),
            )
            .await
            .is_err()
    );
    Ok(())
}
