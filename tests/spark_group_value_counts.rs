use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn group_value_counts_groups_combinations_and_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'eu' AS region, 'free' AS plan
             UNION ALL SELECT 'eu', 'free'
             UNION ALL SELECT 'eu', 'pro'
             UNION ALL SELECT 'us', 'free'
             UNION ALL SELECT 'us', CAST(NULL AS VARCHAR)",
        )
        .await?;

    let grouped = session
        .group_value_counts("events", &["region", "plan"])
        .await?;
    session.register_dataframe("grouped", grouped)?;
    assert_eq!(session.table("grouped").await?.count().await?, 4);
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM grouped WHERE region='eu' AND plan='free'")
            .await?,
        ScalarValue::Int64(Some(2))
    );
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM grouped WHERE plan IS NULL")
            .await?,
        ScalarValue::Int64(Some(1))
    );
    assert!(session.group_value_counts("events", &[]).await.is_err());
    assert!(
        session
            .group_value_counts("events", &["region", "region"])
            .await
            .is_err()
    );
    assert!(
        session
            .group_value_counts("events", &["missing"])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn group_value_counts_rejects_output_alias_collision() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("numbers", "SELECT 1 AS frequency")
        .await?;
    assert!(
        session
            .group_value_counts("numbers", &["frequency"])
            .await
            .is_err()
    );
    Ok(())
}
