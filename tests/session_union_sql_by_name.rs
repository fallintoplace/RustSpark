use rustspark::RustSparkSession;

#[tokio::test]
async fn unions_sql_with_reordered_and_missing_columns()
-> Result<(), Box<dyn std::error::Error>> {
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
    assert!(session.union_sql_by_name(&["SELECT * FROM missing"]).await.is_err());
    Ok(())
}
