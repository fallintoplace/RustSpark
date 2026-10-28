use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn regex_filter_matches_text_and_ignores_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 'alpha' AS \"label text\" UNION ALL
             SELECT 'beta' UNION ALL
             SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;

    assert_eq!(
        session
            .filter_rows_regex("items", "label text", "^a")
            .await?
            .count()
            .await?,
        1
    );
    assert_eq!(
        session
            .filter_rows_regex("items", "label text", "a' OR TRUE --")
            .await?
            .count()
            .await?,
        0
    );
    assert!(
        session
            .filter_rows_regex("items", "missing", ".")
            .await
            .is_err()
    );
    assert!(
        session
            .filter_rows_regex("missing", "label text", ".")
            .await
            .is_err()
    );
    Ok(())
}
