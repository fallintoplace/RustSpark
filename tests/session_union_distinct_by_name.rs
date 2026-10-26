use rustspark::RustSparkSession;

#[tokio::test]
async fn removes_duplicates_across_different_column_orders()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    session
        .register_sql_view("reordered", "SELECT amount, city FROM sales")
        .await?;

    let union = session
        .union_tables_distinct_by_name(&["sales", "reordered"])
        .await?;
    assert_eq!(union.count().await?, 3);
    assert_eq!(
        session
            .union_tables_distinct_by_name(&["sales", "sales"])
            .await?
            .count()
            .await?,
        3
    );
    assert!(session.union_tables_distinct_by_name(&[]).await.is_err());
    assert!(
        session
            .union_tables_distinct_by_name(&["missing"])
            .await
            .is_err()
    );
    Ok(())
}
