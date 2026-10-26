use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn binds_read_only_sql_and_rejects_writes() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    assert_eq!(
        session
            .sql_read_only_with_params(
                "SELECT * FROM sales WHERE amount > $1",
                vec![ScalarValue::Int64(Some(10))]
            )
            .await?
            .count()
            .await?,
        1
    );
    assert!(
        session
            .sql_read_only_with_params("DROP TABLE sales", Vec::<ScalarValue>::new())
            .await
            .is_err()
    );
    assert_eq!(session.table("sales").await?.count().await?, 3);
    Ok(())
}
