use rustspark::RustSparkSession;

#[tokio::test]
async fn ilike_matches_case_insensitively_without_sql_injection()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "labels",
            "SELECT 'Alpha' AS label UNION ALL SELECT 'alpine' UNION ALL SELECT 'BETA' UNION ALL SELECT NULL",
        )
        .await?;
    assert_eq!(
        session
            .filter_rows_ilike("labels", "label", "ALP%")
            .await?
            .count()
            .await?,
        2
    );
    assert_eq!(
        session
            .filter_rows_ilike("labels", "label", "beta")
            .await?
            .count()
            .await?,
        1
    );
    assert_eq!(
        session
            .filter_rows_ilike("labels", "label", "%' OR TRUE --")
            .await?
            .count()
            .await?,
        0
    );
    assert!(
        session
            .filter_rows_ilike("labels", "missing", "%")
            .await
            .is_err()
    );
    assert_eq!(session.table_row_count("labels").await?, 4);
    Ok(())
}
