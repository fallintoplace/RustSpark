use datafusion::logical_expr::JoinType;
use rustspark::{RangeJoinBounds, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn keyed_ranges_match_only_intervals_from_the_same_group() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT 'east' AS region, CAST(0 AS BIGINT) AS moment
             UNION ALL SELECT 'east', 10
             UNION ALL SELECT 'west', 5
             UNION ALL SELECT CAST(NULL AS VARCHAR), 5",
        )
        .await?;
    session
        .register_sql_view(
            "windows",
            "SELECT 'east' AS area, CAST(0 AS BIGINT) AS lower_bound, CAST(10 AS BIGINT) AS upper_bound
             UNION ALL SELECT 'east', 10, 20
             UNION ALL SELECT 'west', 0, 3",
        )
        .await?;
    let keys = &[("region", "area")];
    for (bounds, expected) in [
        (RangeJoinBounds::LeftClosed, 2),
        (RangeJoinBounds::Closed, 3),
    ] {
        let count = session
            .join_tables_on_keyed_range(
                "events",
                "windows",
                JoinType::Inner,
                keys,
                ("moment", ("lower_bound", "upper_bound")),
                bounds,
            )
            .await?
            .count()
            .await?;
        assert_eq!(count, expected, "{bounds:?}");
    }
    assert_eq!(
        session
            .join_tables_on_keyed_range(
                "events",
                "windows",
                JoinType::Left,
                keys,
                ("moment", ("lower_bound", "upper_bound")),
                RangeJoinBounds::LeftClosed,
            )
            .await?
            .count()
            .await?,
        4
    );
    Ok(())
}

#[tokio::test]
async fn keyed_range_rejects_invalid_declarations() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("l", "SELECT 1 AS id, 1 AS moment")
        .await?;
    session
        .register_sql_view("r", "SELECT 1 AS id, 0 AS lo, 2 AS hi")
        .await?;
    for (join_type, keys, value) in [
        (JoinType::Inner, &[][..], "moment"),
        (JoinType::Right, &[("id", "id")][..], "moment"),
        (JoinType::Inner, &[("id", "id")][..], "missing"),
    ] {
        assert!(
            session
                .join_tables_on_keyed_range(
                    "l",
                    "r",
                    join_type,
                    keys,
                    (value, ("lo", "hi")),
                    RangeJoinBounds::Closed,
                )
                .await
                .is_err()
        );
    }
    Ok(())
}
