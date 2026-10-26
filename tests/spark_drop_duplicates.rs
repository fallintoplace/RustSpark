use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn drop_duplicates_supports_full_rows_and_subset_keys() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT CAST(1 AS BIGINT) AS user_id, 'a' AS label
             UNION ALL SELECT 1, 'a'
             UNION ALL SELECT 1, 'b'
             UNION ALL SELECT NULL, 'none'
             UNION ALL SELECT NULL, 'other'",
        )
        .await?;

    let by_key = session.drop_duplicates("events", &["user_id"]).await?;
    assert_eq!(by_key.count().await?, 2);
    let all_columns = session.drop_duplicates("events", &[]).await?;
    assert_eq!(all_columns.count().await?, 4);
    assert_eq!(session.table("events").await?.count().await?, 5);

    assert!(
        session
            .drop_duplicates("events", &["user_id", "USER_ID"])
            .await
            .is_err()
    );
    assert!(
        session
            .drop_duplicates("events", &["missing"])
            .await
            .is_err()
    );
    Ok(())
}
