use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

#[tokio::test]
async fn counts_only_matching_rows_without_collecting_them()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 1 AS score UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT NULL",
        )
        .await?;
    assert_eq!(
        session.count_filtered_rows("scores", col("score").gt(lit(1))).await?,
        2
    );
    assert_eq!(
        session.count_filtered_rows("scores", col("score").gt(lit(10))).await?,
        0
    );
    assert!(session
        .count_filtered_rows("scores", col("missing").eq(lit(1)))
        .await
        .is_err());
    assert_eq!(session.table_row_count("scores").await?, 4);
    Ok(())
}
