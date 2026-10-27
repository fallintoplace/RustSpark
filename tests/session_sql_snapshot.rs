use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn cached_read_only_query_survives_source_removal() -> Result<(), Box<dyn std::error::Error>>
{
    let session = RustSparkSession::new();
    session
        .register_sql_view("source", "SELECT 7 AS value")
        .await?;
    let cached = session.sql_snapshot("SELECT value FROM source").await?;
    assert!(session.deregister_table("source")?);
    assert_eq!(cached.count().await?, 1);

    let parameterized = session
        .sql_snapshot_with_params(
            "SELECT id FROM (VALUES (1), (2)) AS t(id) WHERE id > $1",
            vec![ScalarValue::Int64(Some(1))],
        )
        .await?;
    assert_eq!(parameterized.count().await?, 1);
    assert!(session.sql_snapshot("DROP TABLE something").await.is_err());
    Ok(())
}
