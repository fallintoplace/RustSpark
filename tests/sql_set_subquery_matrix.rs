use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_query(sql: &str, expected: i64) -> TestResult {
    let session = RustSparkSession::new();
    let actual = session.sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn union_all_keeps_duplicates_from_both_sources() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (1),(1),(2),(4)),
             b(v) AS (VALUES (1),(3))
        SELECT CAST(SUM(v) AS BIGINT)
        FROM (
            SELECT v FROM a UNION ALL SELECT v FROM b
        ) combined
    "#;
    assert_query(sql, 12).await
}

#[tokio::test]
async fn union_deduplicates_across_both_branches() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (1),(2),(2)),
             b(v) AS (VALUES (2),(3))
        SELECT CAST(SUM(v) AS BIGINT)
        FROM (
            SELECT v FROM a UNION SELECT v FROM b
        ) combined
    "#;
    assert_query(sql, 6).await
}

#[tokio::test]
async fn in_subquery_matches_multiple_left_duplicates() -> TestResult {
    let sql = r#"
        WITH left_rows(k,v) AS (
            VALUES (1,5),(2,7),(3,5),(4,10)
        ), right_rows(v) AS (
            VALUES (5),(8),(10)
        )
        SELECT CAST(SUM(k) AS BIGINT)
        FROM left_rows WHERE v IN (SELECT v FROM right_rows)
    "#;
    assert_query(sql, 8).await
}

#[tokio::test]
async fn except_removes_distinct_matching_rows() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (1),(2),(3),(4),(4)),
             b(v) AS (VALUES (2),(5))
        SELECT CAST(SUM(v) AS BIGINT)
        FROM (
            SELECT v FROM a EXCEPT SELECT v FROM b
        ) leftover
    "#;
    assert_query(sql, 8).await
}
