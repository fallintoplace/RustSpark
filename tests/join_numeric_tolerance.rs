use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn tolerance_join_matches_nearby_numbers_and_preserves_unmatched_rows() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view(
        "points",
        "SELECT CAST(10 AS BIGINT) AS amount
         UNION ALL SELECT 20
         UNION ALL SELECT 0
         UNION ALL SELECT CAST(NULL AS BIGINT)",
    ).await?;
    session.register_sql_view(
        "targets",
        "SELECT CAST(9 AS BIGINT) AS threshold
         UNION ALL SELECT 21
         UNION ALL SELECT 200",
    ).await?;
    assert_eq!(
        session.join_tables_within_tolerance(
            "points","targets",JoinType::Inner,"amount","threshold",1.0
        ).await?.count().await?,
        2
    );
    assert_eq!(
        session.join_tables_within_tolerance(
            "points","targets",JoinType::Left,"amount","threshold",1.0
        ).await?.count().await?,
        4
    );
    assert_eq!(
        session.join_tables_within_tolerance(
            "points","targets",JoinType::Inner,"amount","threshold",0.0
        ).await?.count().await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn tolerance_join_rejects_invalid_arguments() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view("l", "SELECT 1 AS a").await?;
    session.register_sql_view("r", "SELECT 1 AS b").await?;
    for value in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(
            session.join_tables_within_tolerance("l","r",JoinType::Inner,"a","b",value)
                .await.is_err()
        );
    }
    assert!(
        session.join_tables_within_tolerance("l","r",JoinType::Right,"a","b",1.0)
            .await.is_err()
    );
    assert!(
        session.join_tables_within_tolerance("l","r",JoinType::Inner,"missing","b",1.0)
            .await.is_err()
    );
    Ok(())
}
