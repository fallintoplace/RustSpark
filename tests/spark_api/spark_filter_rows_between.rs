use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn filters_an_inclusive_typed_range() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT CAST(1 AS BIGINT) AS score
             UNION ALL SELECT 2
             UNION ALL SELECT 3
             UNION ALL SELECT 4
             UNION ALL SELECT NULL",
        )
        .await?;
    let filtered = session
        .filter_rows_between(
            "events",
            "score",
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        )
        .await?;
    assert_eq!(filtered.count().await?, 2);
    assert_eq!(session.table("events").await?.count().await?, 5);

    assert!(
        session
            .filter_rows_between(
                "events",
                "missing",
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            )
            .await
            .is_err()
    );
    assert!(
        session
            .filter_rows_between(
                "events",
                "score",
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(2)),
            )
            .await
            .is_err()
    );
    Ok(())
}
