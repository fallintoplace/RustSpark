use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn register_inputs(session: &RustSparkSession) -> TestResult {
    session
        .register_sql_view(
            "left_rows",
            "SELECT id FROM (VALUES (1), (1), (1), (2), (NULL), (NULL)) AS t(id)",
        )
        .await?;
    session
        .register_sql_view(
            "right_rows",
            "SELECT id FROM (VALUES (1), (1), (3), (NULL)) AS t(id)",
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn all_set_operations_preserve_row_multiplicities() -> TestResult {
    let session = RustSparkSession::new();
    register_inputs(&session).await?;

    let common = session.intersect_all_tables("left_rows", "right_rows").await?;
    assert_eq!(common.count().await?, 3);

    let remaining = session.except_all_tables("left_rows", "right_rows").await?;
    assert_eq!(remaining.count().await?, 3);

    let all_distinct = session.intersect_tables("left_rows", "right_rows").await?;
    assert_eq!(all_distinct.count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn symmetric_difference_returns_only_exclusive_distinct_rows() -> TestResult {
    let session = RustSparkSession::new();
    register_inputs(&session).await?;

    let diff = session
        .symmetric_difference_tables("left_rows", "right_rows")
        .await?;
    session.register_dataframe("exclusive", diff)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM exclusive")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT SUM(id) FROM exclusive")
            .await?
            .to_string(),
        "5"
    );
    assert_eq!(
        session
            .symmetric_difference_tables(
                "datafusion.public.left_rows",
                "datafusion.public.right_rows",
            )
            .await?
            .count()
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn extended_set_operations_reject_bad_paths_and_schemas() -> TestResult {
    let session = RustSparkSession::new();
    register_inputs(&session).await?;
    session
        .register_sql_view("wider", "SELECT 1 AS id, 2 AS other")
        .await?;
    assert!(
        session
            .intersect_all_tables("left_rows", "wider")
            .await
            .is_err()
    );
    assert!(
        session
            .except_all_tables("", "right_rows")
            .await
            .is_err()
    );
    assert!(
        session
            .symmetric_difference_tables("left_rows; DROP TABLE right_rows", "right_rows")
            .await
            .is_err()
    );
    assert!(session.table_exists("right_rows")?);
    Ok(())
}
