use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn cardinality_estimate_excludes_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 1 AS value UNION ALL SELECT 1 UNION ALL
             SELECT 2 UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    let estimate = session
        .approx_distinct_column_count("items", "value")
        .await?;
    session.register_dataframe("estimate", estimate)?;
    assert_eq!(
        session
            .sql_scalar("SELECT approximate_distinct BETWEEN 1 AND 3 FROM estimate")
            .await?
            .to_string(),
        "true"
    );
    assert!(
        session
            .approx_distinct_column_count("items", "missing")
            .await
            .is_err()
    );
    Ok(())
}
