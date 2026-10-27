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
async fn half_open_ranges_do_not_double_count_shared_boundaries() -> TestResult {
    let sql = r#"
        WITH p(id,x) AS (
            VALUES (1,0),(2,5),(3,10),(4,15),(5,20)
        ), s(id,lo,hi) AS (
            VALUES (2,0,10),(3,10,20)
        )
        SELECT CAST(SUM(p.id*s.id) AS BIGINT)
        FROM p JOIN s ON p.x >= s.lo AND p.x < s.hi
    "#;
    assert_query(sql, 27).await
}

#[tokio::test]
async fn inclusive_between_matches_both_interval_endpoints() -> TestResult {
    let sql = r#"
        WITH p(id,x) AS (
            VALUES (1,10),(2,20),(3,30)
        ), i(id,lo,hi) AS (
            VALUES (5,10,20),(6,20,30)
        )
        SELECT COUNT(*)
        FROM p JOIN i ON p.x BETWEEN i.lo AND i.hi
    "#;
    assert_query(sql, 4).await
}

#[tokio::test]
async fn self_join_enumerates_only_ordered_pairs() -> TestResult {
    let sql = r#"
        WITH scores(id,score) AS (
            VALUES (1,4),(2,7),(3,7),(4,9)
        )
        SELECT CAST(SUM(a.id*10+b.id) AS BIGINT)
        FROM scores a JOIN scores b
            ON a.id < b.id AND a.score <= b.score
    "#;
    assert_query(sql, 120).await
}

#[tokio::test]
async fn left_join_keeps_unmatched_rows_and_duplicate_matches() -> TestResult {
    let sql = r#"
        WITH a(id,k) AS (
            VALUES (1,1),(2,2),(3,3),(4,4)
        ), b(id,k) AS (
            VALUES (8,2),(9,2),(10,4)
        )
        SELECT CAST(SUM(CASE WHEN b.id IS NULL THEN a.id*10 ELSE b.id END) AS BIGINT)
        FROM a LEFT JOIN b ON a.k=b.k
    "#;
    assert_query(sql, 67).await
}
