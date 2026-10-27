use datafusion::logical_expr::JoinType;
use rustspark::{RangeJoinBounds, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn interval_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "points",
            "SELECT CAST(-1 AS BIGINT) AS moment
             UNION ALL SELECT 0
             UNION ALL SELECT 5
             UNION ALL SELECT 10
             UNION ALL SELECT 15
             UNION ALL SELECT 20
             UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    session
        .register_sql_view(
            "intervals",
            "SELECT CAST(0 AS BIGINT) AS lower_bound, CAST(10 AS BIGINT) AS upper_bound
             UNION ALL SELECT 10, 20",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn interval_join_supports_all_boundary_combinations() -> TestResult {
    let session = interval_session().await?;
    for (bounds, expected) in [
        (RangeJoinBounds::Open, 2),
        (RangeJoinBounds::Closed, 6),
        (RangeJoinBounds::LeftClosed, 4),
        (RangeJoinBounds::RightClosed, 4),
    ] {
        let joined = session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Inner,
                "moment",
                ("lower_bound", "upper_bound"),
                bounds,
            )
            .await?;
        assert_eq!(joined.count().await?, expected, "{bounds:?}");
    }
    Ok(())
}

#[tokio::test]
async fn interval_left_join_preserves_unmatched_and_null_points() -> TestResult {
    let session = interval_session().await?;
    assert_eq!(
        session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Left,
                "moment",
                ("lower_bound", "upper_bound"),
                RangeJoinBounds::LeftClosed,
            )
            .await?
            .count()
            .await?,
        7
    );
    assert_eq!(
        session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Left,
                "moment",
                ("lower_bound", "upper_bound"),
                RangeJoinBounds::Closed,
            )
            .await?
            .count()
            .await?,
        8
    );
    Ok(())
}

#[tokio::test]
async fn interval_join_rejects_invalid_declarations() -> TestResult {
    let session = interval_session().await?;
    assert!(
        session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Right,
                "moment",
                ("lower_bound", "upper_bound"),
                RangeJoinBounds::Open,
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Inner,
                "moment",
                ("", "upper_bound"),
                RangeJoinBounds::Open,
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_range(
                "points",
                "intervals",
                JoinType::Inner,
                "missing",
                ("lower_bound", "upper_bound"),
                RangeJoinBounds::Open,
            )
            .await
            .is_err()
    );
    Ok(())
}
