use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

#[tokio::test]
async fn any_predicate_is_enough_and_nulls_follow_sql_logic()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 1 AS score UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT NULL",
        )
        .await?;
    let filtered = session
        .filter_rows_any(
            "scores",
            &[col("score").eq(lit(1)), col("score").eq(lit(3))],
        )
        .await?;
    assert_eq!(filtered.count().await?, 2);
    assert_eq!(session.table_row_count("scores").await?, 4);
    assert!(session.filter_rows_any("scores", &[]).await.is_err());
    assert!(
        session
            .filter_rows_any("scores", &[col("missing").eq(lit(1))])
            .await
            .is_err()
    );
    Ok(())
}
