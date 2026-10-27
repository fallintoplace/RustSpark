use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn counts_safe_queries_and_respects_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let sql = "SELECT id FROM (VALUES (1), (2), (3)) AS t(id)";
    assert_eq!(session.sql_read_only_row_count(sql).await?, 3);
    assert_eq!(
        session
            .sql_read_only_row_count_with_params(
                "SELECT id FROM (VALUES (1), (2), (3)) AS t(id) WHERE id > $1",
                vec![ScalarValue::Int64(Some(1))],
            )
            .await?,
        2
    );
    assert_eq!(
        session
            .sql_read_only_row_count("SELECT 1 WHERE FALSE")
            .await?,
        0
    );
    assert!(
        session
            .sql_read_only_row_count("CREATE TABLE bad AS SELECT 1")
            .await
            .is_err()
    );
    assert!(!session.table_exists("bad")?);
    Ok(())
}
