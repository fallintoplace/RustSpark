use datafusion::logical_expr::JoinType;
use rustspark::{RangeJoinBounds, RangeTableJoin, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view(
        "events", "SELECT CAST(5 AS BIGINT) AS value
                   UNION ALL SELECT 10
                   UNION ALL SELECT 15
                   UNION ALL SELECT CAST(NULL AS BIGINT)"
    ).await?;
    session.register_sql_view(
        "buckets", "SELECT CAST(0 AS BIGINT) AS low, CAST(10 AS BIGINT) AS high, 'a' AS bucket
                   UNION ALL SELECT 10, 20, 'b'"
    ).await?;
    session.register_sql_view(
        "campaigns", "SELECT CAST(5 AS BIGINT) AS start_value, CAST(15 AS BIGINT) AS end_value, 'promo' AS tag"
    ).await?;
    Ok(session)
}

#[tokio::test]
async fn joins_three_tables_with_half_open_bounds_and_left_rows() -> TestResult {
    let session = fixture().await?;
    let frame = session.join_many_range_tables("events", "e", &[
        RangeTableJoin {
            table: "buckets", alias: "b", join_type: JoinType::Left,
            value_column: "value", lower_column: "low", upper_column: "high",
            bounds: RangeJoinBounds::LeftClosed,
        },
        RangeTableJoin {
            table: "campaigns", alias: "c", join_type: JoinType::Left,
            value_column: "value", lower_column: "start_value", upper_column: "end_value",
            bounds: RangeJoinBounds::LeftClosed,
        },
    ]).await?;
    assert_eq!(frame.count().await?, 4);
    Ok(())
}

#[tokio::test]
async fn inner_ranges_require_both_interval_matches() -> TestResult {
    let session = fixture().await?;
    let frame = session.join_many_range_tables("events", "e", &[
        RangeTableJoin {
            table: "buckets", alias: "b", join_type: JoinType::Inner,
            value_column: "value", lower_column: "low", upper_column: "high",
            bounds: RangeJoinBounds::LeftClosed,
        },
        RangeTableJoin {
            table: "campaigns", alias: "c", join_type: JoinType::Inner,
            value_column: "value", lower_column: "start_value", upper_column: "end_value",
            bounds: RangeJoinBounds::LeftClosed,
        },
    ]).await?;
    assert_eq!(frame.count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn closed_range_includes_both_endpoints() -> TestResult {
    let session = fixture().await?;
    let frame = session.join_many_range_tables("events", "e", &[
        RangeTableJoin {
            table: "campaigns", alias: "c", join_type: JoinType::Inner,
            value_column: "value", lower_column: "start_value", upper_column: "end_value",
            bounds: RangeJoinBounds::Closed,
        },
    ]).await?;
    assert_eq!(frame.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn rejects_duplicate_aliases_blank_columns_and_unsupported_types() -> TestResult {
    let session = fixture().await?;
    assert!(session.join_many_range_tables("events", "e", &[]).await.is_err());
    for spec in [
        RangeTableJoin {
            table: "buckets", alias: "e", join_type: JoinType::Left,
            value_column: "value", lower_column: "low", upper_column: "high",
            bounds: RangeJoinBounds::Open,
        },
        RangeTableJoin {
            table: "buckets", alias: "b", join_type: JoinType::Full,
            value_column: "value", lower_column: "low", upper_column: "high",
            bounds: RangeJoinBounds::Open,
        },
        RangeTableJoin {
            table: "buckets", alias: "b", join_type: JoinType::Inner,
            value_column: "", lower_column: "low", upper_column: "high",
            bounds: RangeJoinBounds::Open,
        },
    ] {
        assert!(session.join_many_range_tables("events", "e", &[spec]).await.is_err());
    }
    Ok(())
}
