use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn having_detects_groups_with_missing_values() -> TestResult {
    let sql = r#"
        WITH readings(region, score) AS
            (VALUES ('N', 5), ('N', NULL), ('S', NULL), ('S', NULL), ('W', 3))
        SELECT COUNT(*) FROM (
            SELECT region FROM readings
            GROUP BY region HAVING COUNT(*) > COUNT(score)
        ) missing
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn filtered_aggregates_keep_predicates_separate() -> TestResult {
    let sql = r#"
        WITH payments(kind, amount) AS
            (VALUES ('paid', 5), ('paid', 8), ('refund', 4), ('paid', NULL))
        SELECT COUNT(*) FROM (
            SELECT SUM(amount) FILTER (WHERE kind = 'paid') AS paid,
                   SUM(amount) FILTER (WHERE kind = 'refund') AS refunds
            FROM payments
        ) summary WHERE paid = 13 AND refunds = 4
    "#;
    assert_scalar(sql, 1).await
}

#[tokio::test]
async fn except_all_subtracts_duplicates_and_nulls() -> TestResult {
    let sql = r#"
        WITH a(value) AS (VALUES (1), (1), (2), (NULL), (NULL)),
        b(value) AS (VALUES (1), (NULL))
        SELECT COUNT(*) FROM (
            SELECT value FROM a EXCEPT ALL SELECT value FROM b
        ) difference
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn cte_union_all_keeps_group_totals() -> TestResult {
    let sql = r#"
        WITH source(kind, amount) AS
            (VALUES ('x', 2), ('x', 3), ('y', 10)),
        totals AS (
            SELECT kind, SUM(amount) AS total FROM source GROUP BY kind
        ),
        boosted AS (
            SELECT kind, total FROM totals UNION ALL SELECT 'z', 7
        )
        SELECT COUNT(*) FROM boosted WHERE total >= 7
    "#;
    assert_scalar(sql, 2).await
}
