use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn top_frequencies_are_sorted_and_bounded() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "fruit",
            "SELECT 'apple' AS name UNION ALL SELECT 'apple'
             UNION ALL SELECT 'apple' UNION ALL SELECT 'pear'
             UNION ALL SELECT 'pear' UNION ALL SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;

    let top = session.top_frequent_values("fruit", "name", 1).await?;
    session.register_dataframe("popular", top)?;
    assert_eq!(session.table("popular").await?.count().await?, 1);
    assert_eq!(
        session.sql_scalar("SELECT frequency FROM popular").await?,
        ScalarValue::Int64(Some(3))
    );
    assert_eq!(
        session
            .sql_scalar("SELECT value FROM popular")
            .await?
            .to_string(),
        "apple"
    );
    assert_eq!(
        session
            .top_frequent_values("fruit", "name", 0)
            .await?
            .count()
            .await?,
        0
    );
    assert_eq!(
        session
            .top_frequent_values("fruit", "name", 100)
            .await?
            .count()
            .await?,
        3
    );
    assert!(
        session
            .top_frequent_values("fruit", "missing", 1)
            .await
            .is_err()
    );
    Ok(())
}
