use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn try_cast_failure_uses_coalesce_fallback() -> TestResult {
    let sql = r#"
        SELECT COUNT(*) FROM (VALUES (1)) AS input(value)
        WHERE COALESCE(TRY_CAST('invalid' AS BIGINT), CAST(17 AS BIGINT)) = 17
    "#;
    assert_scalar(sql, 1).await
}

#[tokio::test]
async fn unicode_length_distinguishes_characters_from_bytes() -> TestResult {
    let sql = r#"
        SELECT COUNT(*) FROM (VALUES (1)) AS input(value)
        WHERE character_length('naïve') = 5
          AND octet_length('naïve') = 6
    "#;
    assert_scalar(sql, 1).await
}

#[tokio::test]
async fn date_extraction_handles_leap_day() -> TestResult {
    let sql = r#"
        SELECT COUNT(*) FROM (VALUES (1)) AS input(value)
        WHERE EXTRACT(YEAR FROM CAST('2024-02-29' AS DATE)) = 2024
          AND EXTRACT(DAY FROM CAST('2024-02-29' AS DATE)) = 29
    "#;
    assert_scalar(sql, 1).await
}

#[tokio::test]
async fn trim_lower_and_regex_replace_compose() -> TestResult {
    let sql = r#"
        SELECT COUNT(*) FROM (VALUES (1)) AS input(value)
        WHERE lower(trim('  MIXED  ')) = 'mixed'
          AND regexp_replace('ab123', '[0-9]+', 'X') = 'abX'
    "#;
    assert_scalar(sql, 1).await
}
