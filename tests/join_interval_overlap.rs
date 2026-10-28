use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn interval_overlap_distinguishes_touching_endpoints() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "l",
            "SELECT CAST(0 AS BIGINT) AS start_at, CAST(5 AS BIGINT) AS end_at
             UNION ALL SELECT 5, 10
             UNION ALL SELECT 20, 30
             UNION ALL SELECT CAST(NULL AS BIGINT), 3
             UNION ALL SELECT 6, 6",
        )
        .await?;
    session
        .register_sql_view(
            "r",
            "SELECT CAST(5 AS BIGINT) AS from_at, CAST(7 AS BIGINT) AS until_at
             UNION ALL SELECT 25, 27",
        )
        .await?;
    for (join_type, include_touching, expected) in [
        (JoinType::Inner, false, 2),
        (JoinType::Inner, true, 4),
        (JoinType::Left, false, 5),
    ] {
        let count = session
            .join_tables_on_interval_overlap(
                "l",
                "r",
                join_type,
                ("start_at", "end_at"),
                ("from_at", "until_at"),
                include_touching,
            )
            .await?
            .count()
            .await?;
        assert_eq!(count, expected, "{join_type:?} touching={include_touching}");
    }
    Ok(())
}

#[tokio::test]
async fn interval_overlap_rejects_unsupported_types_and_columns() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("l", "SELECT 0 AS start_at, 1 AS end_at")
        .await?;
    session
        .register_sql_view("r", "SELECT 0 AS lo, 1 AS hi")
        .await?;
    assert!(
        session
            .join_tables_on_interval_overlap(
                "l",
                "r",
                JoinType::Full,
                ("start_at", "end_at"),
                ("lo", "hi"),
                false,
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_interval_overlap(
                "l",
                "r",
                JoinType::Inner,
                ("missing", "end_at"),
                ("lo", "hi"),
                false,
            )
            .await
            .is_err()
    );
    Ok(())
}
