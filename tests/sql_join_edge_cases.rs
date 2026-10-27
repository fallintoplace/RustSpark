use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn left_join_composite_keys_preserves_unmatched_rows() -> TestResult {
    let sql = r#"
        WITH left_rows(id, code) AS
            (VALUES (1, 'a'), (1, 'b'), (2, 'c'), (NULL, 'd')),
        right_rows(id, code, value) AS
            (VALUES (1, 'a', 8), (1, 'a', 9), (2, 'd', 5))
        SELECT COUNT(*) FROM left_rows l
        LEFT JOIN right_rows r ON l.id = r.id AND l.code = r.code
        WHERE r.value IS NULL
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn join_duplicates_aggregate_before_having() -> TestResult {
    let sql = r#"
        WITH orders(id, region) AS
            (VALUES (1, 'EU'), (2, 'EU'), (3, 'US')),
        lines(order_id, amount) AS
            (VALUES (1, 5), (1, 7), (2, 2), (2, 3), (3, 10))
        SELECT SUM(total) FROM (
            SELECT o.region, SUM(l.amount) AS total
            FROM orders o JOIN lines l ON o.id = l.order_id
            GROUP BY o.region HAVING COUNT(*) >= 2
        ) grouped
    "#;
    assert_scalar(sql, 17).await
}

#[tokio::test]
async fn correlated_not_exists_ignores_nonmatching_and_null_scores() -> TestResult {
    let sql = r#"
        WITH users(id) AS (VALUES (1), (2), (3), (4)),
        events(user_id, score) AS
            (VALUES (1, 7), (1, 1), (2, 5), (3, NULL), (NULL, 9))
        SELECT COUNT(*) FROM users u
        WHERE NOT EXISTS (
            SELECT 1 FROM events e
            WHERE e.user_id = u.id AND e.score > 5
        )
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn null_safe_equality_matches_null_join_keys() -> TestResult {
    let sql = r#"
        WITH l(k) AS (VALUES (1), (NULL), (NULL)),
        r(k) AS (VALUES (NULL), (1))
        SELECT COUNT(*) FROM l JOIN r
        ON l.k IS NOT DISTINCT FROM r.k
    "#;
    assert_scalar(sql, 3).await
}
