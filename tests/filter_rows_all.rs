use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

#[tokio::test]
async fn all_predicates_are_applied_without_mutating_source()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 1 AS score UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT NULL",
        )
        .await?;
    let filtered = session
        .filter_rows_all("scores", &[col("score").gt(lit(1)), col("score").lt(lit(3))])
        .await?;
    assert_eq!(filtered.count().await?, 1);
    assert_eq!(session.table_row_count("scores").await?, 4);
    assert!(session.filter_rows_all("scores", &[]).await.is_err());
    assert!(session
        .filter_rows_all("scores", &[col("missing").eq(lit(1))])
        .await
        .is_err());
    Ok(())
}
