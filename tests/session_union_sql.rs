use rustspark::RustSparkSession;

#[tokio::test]
async fn unions_sql_by_position_and_keeps_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let combined = session
        .union_sql(&[
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(1 AS BIGINT) AS value",
            "SELECT CAST(2 AS BIGINT) AS value",
        ])
        .await?;
    assert_eq!(combined.count().await?, 3);
    assert!(session.union_sql(&[]).await.is_err());
    assert!(
        session
            .union_sql(&["SELECT 1 AS a", "SELECT 1 AS a, 2 AS b"])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn positional_union_plans_more_than_one_concurrency_window()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let queries: Vec<String> = (0..20)
        .map(|index| format!("SELECT {index} AS value"))
        .collect();
    let references: Vec<&str> = queries.iter().map(String::as_str).collect();
    let dataframe = session.union_sql(&references).await?;
    assert_eq!(dataframe.clone().count().await?, 20);
    let expected = session.sql_scalar("SELECT 190").await?;
    session.register_dataframe("all_values", dataframe)?;
    assert_eq!(
        session
            .sql_scalar("SELECT SUM(value) FROM all_values")
            .await?,
        expected
    );
    Ok(())
}
