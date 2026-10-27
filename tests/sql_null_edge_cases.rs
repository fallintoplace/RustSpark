use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn not_in_with_null_list_rejects_all_rows() -> TestResult {
    let sql = r#"
        WITH ids(id) AS (VALUES (1), (2), (NULL))
        SELECT COUNT(*) FROM ids WHERE id NOT IN (1, NULL)
    "#;
    assert_scalar(sql, 0).await
}

#[tokio::test]
async fn distinct_from_counts_null_as_distinct() -> TestResult {
    let sql = r#"
        WITH ids(id) AS (VALUES (1), (2), (NULL))
        SELECT COUNT(*) FROM ids WHERE id IS DISTINCT FROM 1
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn distinct_count_ignores_nulls_but_count_star_does_not() -> TestResult {
    let sql = r#"
        WITH amounts(value) AS (VALUES (1), (1), (2), (NULL), (NULL))
        SELECT COUNT(*) - COUNT(value) + COUNT(DISTINCT value) FROM amounts
    "#;
    assert_scalar(sql, 4).await
}

#[tokio::test]
async fn case_range_keeps_null_out_of_sum() -> TestResult {
    let sql = r#"
        WITH readings(value) AS (VALUES (10), (20), (30), (NULL))
        SELECT SUM(CASE WHEN value BETWEEN 10 AND 20 THEN value ELSE 0 END)
        FROM readings
    "#;
    assert_scalar(sql, 30).await
}
