use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn distinct_columns_projects_and_deduplicates_lazily() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    let cities = session.distinct_columns("sales", &["city"]).await?;
    assert_eq!(cities.count().await?, 2);

    let both = session
        .distinct_columns("sales", &["amount", "city"])
        .await?;
    let names: Vec<_> = both
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["amount", "city"]);
    assert_eq!(both.count().await?, 3);

    assert!(session.distinct_columns("sales", &[]).await.is_err());
    assert!(
        session
            .distinct_columns("sales", &["missing"])
            .await
            .is_err()
    );
    assert!(
        session
            .distinct_columns("sales", &["city", "CITY"])
            .await
            .is_err()
    );
    assert_eq!(session.table("sales").await?.count().await?, 3);
    Ok(())
}
