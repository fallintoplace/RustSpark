use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn dropna_threshold_counts_non_null_values() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 1 AS a, CAST(NULL AS BIGINT) AS b, CAST(NULL AS BIGINT) AS c
             UNION ALL SELECT 2, 3, NULL
             UNION ALL SELECT NULL, 5, 6",
        )
        .await?;

    assert_eq!(
        session
            .drop_null_rows_with_threshold("items", &[], 2)
            .await?
            .count()
            .await?,
        2
    );
    assert_eq!(
        session
            .drop_null_rows_with_threshold("items", &["a", "b"], 2)
            .await?
            .count()
            .await?,
        1
    );
    assert_eq!(
        session
            .drop_null_rows_with_threshold("items", &[], 0)
            .await?
            .count()
            .await?,
        3
    );
    assert_eq!(
        session
            .drop_null_rows_with_threshold("items", &[], 3)
            .await?
            .count()
            .await?,
        0
    );
    assert!(
        session
            .drop_null_rows_with_threshold("items", &[], 4)
            .await
            .is_err()
    );
    assert!(
        session
            .drop_null_rows_with_threshold("items", &["missing"], 0)
            .await
            .is_err()
    );
    Ok(())
}
