use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn dense_rank_does_not_skip_positions_after_ties() -> TestResult {
    let sql = r#"
        WITH scores(team, points) AS
            (VALUES ('A', 30), ('A', 30), ('A', 10), ('B', 50), ('B', 20))
        SELECT COUNT(*) FROM (
            SELECT DENSE_RANK() OVER (
                PARTITION BY team ORDER BY points DESC
            ) AS rank_num FROM scores
        ) ranked WHERE rank_num = 2
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn rolling_rows_window_resets_per_partition() -> TestResult {
    let sql = r#"
        WITH sales(group_id, day, amount) AS
            (VALUES ('A', 1, 2), ('A', 2, 3), ('A', 3, 5),
                    ('B', 1, 7), ('B', 2, 11))
        SELECT COUNT(*) FROM (
            SELECT group_id, day,
                SUM(amount) OVER (
                    PARTITION BY group_id ORDER BY day
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) AS rolling
            FROM sales
        ) w WHERE (group_id = 'A' AND day = 3 AND rolling = 8)
              OR (group_id = 'B' AND day = 2 AND rolling = 18)
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn lag_defaults_reset_at_partition_boundaries() -> TestResult {
    let sql = r#"
        WITH events(group_id, seq, value) AS
            (VALUES ('A', 1, 4), ('A', 2, 6),
                    ('B', 1, 10), ('B', 2, 20))
        SELECT COUNT(*) FROM (
            SELECT group_id, seq,
                LAG(value, 1, 0) OVER (
                    PARTITION BY group_id ORDER BY seq
                ) AS previous_value
            FROM events
        ) w WHERE (seq = 1 AND previous_value = 0)
              OR (group_id = 'B' AND seq = 2 AND previous_value = 10)
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn row_number_breaks_ties_deterministically() -> TestResult {
    let sql = r#"
        WITH scores(team, player, points) AS
            (VALUES ('A', 'Ada', 15), ('A', 'Ben', 15), ('A', 'Cat', 9),
                    ('B', 'Dan', 6), ('B', 'Eve', 8))
        SELECT COUNT(*) FROM (
            SELECT team, player, ROW_NUMBER() OVER (
                PARTITION BY team ORDER BY points DESC, player ASC
            ) AS row_num FROM scores
        ) ranked WHERE row_num = 1 AND player IN ('Ada', 'Eve')
    "#;
    assert_scalar(sql, 2).await
}
