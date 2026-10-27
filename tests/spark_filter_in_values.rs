use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn typed_in_filter_handles_nulls_empty_sets_and_unknown_columns() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT id FROM (VALUES (1), (2), (3), (NULL)) AS t(id)",
        )
        .await?;

    let matching = session
        .filter_rows_in_values(
            "events",
            "id",
            &[ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(3))],
        )
        .await?;
    assert_eq!(matching.count().await?, 2);
    assert_eq!(
        session.filter_rows_in_values("events", "id", &[]).await?.count().await?,
        0
    );
    assert!(
        session
            .filter_rows_in_values("events", "missing", &[ScalarValue::Int64(Some(1))])
            .await
            .is_err()
    );
    assert_eq!(session.table("events").await?.count().await?, 4);
    Ok(())
}
