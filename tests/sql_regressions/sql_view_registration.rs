use rustspark::RustSparkSession;

#[tokio::test]
async fn registers_a_query_as_a_lazy_view() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv("sales", "tests/fixtures/sales.csv")
        .await?;

    session
        .register_sql_view(
            "large_sales",
            "SELECT city, amount FROM sales WHERE amount >= 10",
        )
        .await?;

    assert_eq!(session.table("large_sales").await?.count().await?, 2);
    assert_eq!(
        session
            .sql("SELECT * FROM large_sales WHERE city = 'Berlin'")
            .await?
            .count()
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn invalid_view_sql_does_not_register_a_table() {
    let session = RustSparkSession::new();

    assert!(
        session
            .register_sql_view("broken", "SELECT * FROM nonexistent")
            .await
            .is_err()
    );
    assert!(session.table("broken").await.is_err());
}
