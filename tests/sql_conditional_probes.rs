use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn conditional_division_ignores_zero_denominators() -> TestResult {
    let sql = r#"
        WITH nums(numerator,denominator) AS (VALUES (8,2),(9,0),(15,3),(0,0))
         SELECT COUNT(*) FROM nums WHERE CASE WHEN denominator=0 THEN NULL ELSE numerator/denominator END >=4
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn three_valued_boolean_predicates_filter_unknown() -> TestResult {
    let sql = r#"
        WITH flags(a,b) AS (VALUES (1,1),(1,NULL),(0,1),(NULL,1),(NULL,NULL))
         SELECT COUNT(*) FROM flags WHERE (a=1 OR b=1) AND NOT (a=0)
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn coalesce_nullif_supports_nonzero_fallback() -> TestResult {
    let sql = r#"
        WITH nums(n) AS (VALUES (0),(3),(NULL),(-2),(0))
         SELECT COUNT(*) FROM nums WHERE COALESCE(NULLIF(n,0),7)>0
    "#;
    assert_scalar(sql, 4).await
}

#[tokio::test]
async fn date_filter_handles_february_boundary() -> TestResult {
    let sql = r#"
        WITH dates(day) AS (VALUES ('2024-02-28'),('2024-02-29'),('2024-03-01'),('2023-02-28'))
         SELECT COUNT(*) FROM dates WHERE day BETWEEN '2024-02-28' AND '2024-02-29'
    "#;
    assert_scalar(sql, 2).await
}
