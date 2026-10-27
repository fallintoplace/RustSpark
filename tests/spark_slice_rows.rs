use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn slice_rows_lazily_paginates_an_ordered_table() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "ordered_rows",
            "SELECT id FROM (VALUES (4), (1), (3), (2)) AS t(id) ORDER BY id",
        )
        .await?;
    let page = session.slice_rows("ordered_rows", 1, 2).await?;
    session.register_dataframe("page", page)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM page")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT MIN(id) FROM page")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT MAX(id) FROM page")
            .await?
            .to_string(),
        "3"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM ordered_rows")
            .await?
            .to_string(),
        "4"
    );
    Ok(())
}

#[tokio::test]
async fn slice_rows_supports_zero_and_beyond_end() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("numbers", "SELECT 1 AS id UNION ALL SELECT 2")
        .await?;
    assert_eq!(session.slice_rows("numbers", 0, 0).await?.count().await?, 0);
    assert_eq!(
        session.slice_rows("numbers", 10, 3).await?.count().await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn slice_rows_rejects_overflow_and_missing_tables() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("numbers", "SELECT 1 AS id")
        .await?;
    let err = session
        .slice_rows("numbers", usize::MAX, 1)
        .await
        .err()
        .ok_or("overflow should fail")?;
    assert!(err.to_string().contains("offset plus length"));
    assert!(session.slice_rows("missing", 0, 1).await.is_err());
    Ok(())
}
