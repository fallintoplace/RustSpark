use rustspark::{NullDropMode, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn dropna_filters_any_or_all_null_values() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "rows",
            "SELECT CAST(1 AS BIGINT) AS a, CAST(NULL AS BIGINT) AS b
             UNION ALL SELECT NULL, 2
             UNION ALL SELECT NULL, NULL
             UNION ALL SELECT 1, 2",
        )
        .await?;

    let any = session.drop_null_rows("rows", &[], NullDropMode::Any).await?;
    assert_eq!(any.count().await?, 1);
    let all = session.drop_null_rows("rows", &[], NullDropMode::All).await?;
    assert_eq!(all.count().await?, 3);
    let subset = session
        .drop_null_rows("rows", &["a"], NullDropMode::Any)
        .await?;
    assert_eq!(subset.count().await?, 2);
    assert_eq!(session.table("rows").await?.count().await?, 4);

    assert!(
        session
            .drop_null_rows("rows", &["not_a_column"], NullDropMode::Any)
            .await
            .is_err()
    );
    assert!(
        session
            .drop_null_rows("rows", &["a", "A"], NullDropMode::All)
            .await
            .is_err()
    );
    Ok(())
}
