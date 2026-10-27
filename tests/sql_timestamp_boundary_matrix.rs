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
async fn hour_bucket_excludes_next_hour_boundary() -> TestResult {
    let sql = r#"
        WITH events(sec) AS (
            VALUES (59),(60),(3599),(3600),(3660)
        )
        SELECT COUNT(*) FROM events
        WHERE DATE_TRUNC('hour', TO_TIMESTAMP_SECONDS(sec)) = TO_TIMESTAMP_SECONDS(0)
    "#;
    assert_query(sql, 3).await
}

#[tokio::test]
async fn minute_bucket_includes_whole_minute_only() -> TestResult {
    let sql = r#"
        WITH events(id,sec) AS (
            VALUES (1,59),(2,60),(3,119),(4,120)
        )
        SELECT CAST(SUM(id) AS BIGINT) FROM events
        WHERE DATE_TRUNC('minute', TO_TIMESTAMP_SECONDS(sec)) = TO_TIMESTAMP_SECONDS(60)
    "#;
    assert_query(sql, 5).await
}

#[tokio::test]
async fn calendar_year_extraction_distinguishes_three_years() -> TestResult {
    let sql = r#"
        WITH events(sec) AS (
            VALUES (1672531200),(1704067200),(1735689600)
        )
        SELECT COUNT(DISTINCT EXTRACT(YEAR FROM TO_TIMESTAMP_SECONDS(sec)))
        FROM events
    "#;
    assert_query(sql, 3).await
}

#[tokio::test]
async fn timestamp_half_open_filter_respects_day_boundaries() -> TestResult {
    let sql = r#"
        WITH events(id,sec) AS (
            VALUES (1,86399),(2,86400),(3,172799),(4,172800)
        )
        SELECT CAST(SUM(id) AS BIGINT) FROM events
        WHERE TO_TIMESTAMP_SECONDS(sec) >= TO_TIMESTAMP_SECONDS(86400)
          AND TO_TIMESTAMP_SECONDS(sec) < TO_TIMESTAMP_SECONDS(172800)
    "#;
    assert_query(sql, 5).await
}
