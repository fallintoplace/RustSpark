use datafusion::prelude::{col, lit};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn filter_rows_uses_typed_predicates_and_sql_null_semantics() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT CAST(1 AS BIGINT) AS id
             UNION ALL SELECT 2
             UNION ALL SELECT NULL",
        )
        .await?;

    let larger = session
        .filter_rows("events", col("id").gt(lit(1i64)))
        .await?;
    assert_eq!(larger.count().await?, 1);
    let including_nulls = session
        .filter_rows("events", col("id").is_null().or(col("id").eq(lit(1i64))))
        .await?;
    assert_eq!(including_nulls.count().await?, 2);

    assert!(
        session
            .filter_rows("events", col("unknown").eq(lit(1i64)))
            .await
            .is_err()
    );
    assert!(
        session
            .filter_rows("events", lit(1i64))
            .await
            .is_err()
    );
    assert_eq!(session.table("events").await?.count().await?, 3);
    Ok(())
}
