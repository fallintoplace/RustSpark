use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn materialized_sql_view_survives_deleted_source() -> TestResult {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("values.csv");
    std::fs::write(&file, "value\n1\n2\n3\n")?;

    let session = RustSparkSession::new();
    session.register_csv("numbers", &file).await?;
    session
        .register_materialized_sql_view("larger", "SELECT value FROM numbers WHERE value > 1")
        .await?;

    std::fs::remove_file(&file)?;
    assert_eq!(session.table("larger").await?.count().await?, 2);
    assert_eq!(
        session
            .sql("SELECT SUM(value) FROM larger")
            .await?
            .count()
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn rejected_materialized_views_do_not_replace_existing_tables() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_materialized_sql_view("present", "SELECT 42 AS value")
        .await?;

    assert!(
        session
            .register_materialized_sql_view("present", "SELECT 9 AS value")
            .await
            .is_err()
    );
    assert!(
        session
            .register_materialized_sql_view("missing_view", "SELECT * FROM unknown_table")
            .await
            .is_err()
    );
    assert_eq!(
        session
            .sql("SELECT value FROM present WHERE value = 42")
            .await?
            .count()
            .await?,
        1
    );
    assert!(session.table("missing_view").await.is_err());
    Ok(())
}
