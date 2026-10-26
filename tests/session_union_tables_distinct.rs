use rustspark::RustSparkSession;

#[tokio::test]
async fn distinct_positional_union_removes_repeated_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    assert_eq!(
        session
            .union_tables_distinct(&["sales", "sales"])
            .await?
            .count()
            .await?,
        3
    );
    assert_eq!(session.union_tables_distinct(&["sales"]).await?.count().await?, 3);
    assert!(session.union_tables_distinct(&[]).await.is_err());
    session
        .register_sql_view("just_city", "SELECT city FROM sales")
        .await?;
    assert!(session.union_tables_distinct(&["sales", "just_city"]).await.is_err());
    Ok(())
}
