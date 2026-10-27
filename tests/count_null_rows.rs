use rustspark::{NullDropMode, RustSparkSession};

#[tokio::test]
async fn counts_missing_rows_with_any_or_all_null_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "rows",
            "SELECT CAST(NULL AS INT) AS a, 'x' AS b
             UNION ALL SELECT 1, NULL
             UNION ALL SELECT NULL, NULL
             UNION ALL SELECT 2, 'ok'",
        )
        .await?;
    assert_eq!(session.count_null_rows("rows", &["a", "b"], NullDropMode::Any).await?, 3);
    assert_eq!(session.count_null_rows("rows", &["a", "b"], NullDropMode::All).await?, 1);
    assert_eq!(session.count_null_rows("rows", &[], NullDropMode::Any).await?, 3);
    assert!(session.count_null_rows("rows", &["missing"], NullDropMode::Any).await.is_err());
    assert_eq!(session.table_row_count("rows").await?, 4);
    Ok(())
}
