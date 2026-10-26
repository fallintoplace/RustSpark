use rustspark::RustSparkSession;

#[tokio::test]
async fn unions_sql_with_reordered_and_missing_columns() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    let combined = session
        .union_sql_by_name(&[
            "SELECT city, amount FROM sales WHERE amount >= 10",
            "SELECT amount, city FROM sales WHERE amount < 10",
        ])
        .await?;
    assert_eq!(combined.count().await?, 3);

    let evolving = session
        .union_sql_by_name(&[
            "SELECT city FROM sales WHERE amount >= 10",
            "SELECT amount FROM sales WHERE amount < 10",
        ])
        .await?;
    assert_eq!(evolving.schema().fields().len(), 2);
    assert_eq!(evolving.count().await?, 3);
    assert!(session.union_sql_by_name(&[]).await.is_err());
    assert!(
        session
            .union_sql_by_name(&["SELECT * FROM missing"])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn name_union_plans_more_than_one_concurrency_window()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let queries: Vec<String> = (0..20)
        .map(|index| {
            if index % 2 == 0 {
                format!("SELECT {index} AS a, 1 AS b")
            } else {
                format!("SELECT 1 AS b, {index} AS a")
            }
        })
        .collect();
    let references: Vec<&str> = queries.iter().map(String::as_str).collect();
    let dataframe = session.union_sql_by_name(&references).await?;
    assert_eq!(dataframe.clone().count().await?, 20);
    assert_eq!(dataframe.schema().field(0).name(), "a");
    session.register_dataframe("union_values", dataframe)?;
    let actual = session
        .sql_scalar("SELECT SUM(a) FROM union_values")
        .await?;
    let expected = session.sql_scalar("SELECT 190").await?;
    assert_eq!(actual, expected);
    Ok(())
}
