use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn select_columns_projects_in_requested_order_without_changing_source() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(5 AS BIGINT) AS amount, 'Berlin' AS city, TRUE AS active",
        )
        .await?;

    let selected = session.select_columns("items", &["city", "amount"]).await?;
    let names: Vec<_> = selected
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["city", "amount"]);
    assert_eq!(selected.count().await?, 1);
    assert_eq!(session.describe_table("items").await?.len(), 3);

    assert!(session.select_columns("items", &[]).await.is_err());
    assert!(session.select_columns("items", &["missing"]).await.is_err());
    assert!(
        session
            .select_columns("items", &["city", "CITY"])
            .await
            .is_err()
    );
    Ok(())
}
