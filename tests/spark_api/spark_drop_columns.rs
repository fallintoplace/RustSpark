use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn drop_columns_ignores_missing_names_without_changing_source() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(7 AS BIGINT) AS id, 'green' AS color, TRUE AS active",
        )
        .await?;

    let dropped = session
        .drop_columns("items", &["color", "not_present", "color"])
        .await?;
    let names: Vec<_> = dropped
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["id", "active"]);
    assert_eq!(dropped.count().await?, 1);

    assert_eq!(
        session
            .drop_columns("items", &[])
            .await?
            .schema()
            .fields()
            .len(),
        3
    );
    assert!(
        session
            .drop_columns("items", &["id", "color", "active"])
            .await
            .is_err()
    );
    assert_eq!(session.describe_table("items").await?.len(), 3);
    Ok(())
}
