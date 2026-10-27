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
async fn square_of_group_totals_preserves_negative_contributions() -> TestResult {
    let sql = r#"
        WITH lines(customer, amount) AS (
            VALUES (1,10),(1,-4),(2,3),(2,3),(3,-1)
        )
        SELECT CAST(SUM(total*total) AS BIGINT)
        FROM (
            SELECT customer, SUM(amount) AS total
            FROM lines GROUP BY customer
        ) totals
    "#;
    assert_query(sql, 73).await
}

#[tokio::test]
async fn case_weighted_sum_keeps_zero_and_negative_amounts() -> TestResult {
    let sql = r#"
        WITH data(amount) AS (
            VALUES (10),(0),(-3),(5),(-8)
        )
        SELECT CAST(SUM(CASE WHEN amount >= 0 THEN amount*2 ELSE amount END) AS BIGINT)
        FROM data
    "#;
    assert_query(sql, 19).await
}

#[tokio::test]
async fn grouped_having_filters_before_outer_aggregate() -> TestResult {
    let sql = r#"
        WITH line(k,x) AS (
            VALUES (1,10),(1,5),(2,4),(3,7),(3,9),(3,2)
        )
        SELECT CAST(SUM(k*100+total) AS BIGINT)
        FROM (
            SELECT k, SUM(x) AS total
            FROM line GROUP BY k HAVING COUNT(*) >= 2
        ) grouped
    "#;
    assert_query(sql, 433).await
}

#[tokio::test]
async fn group_min_max_spread_handles_signed_data() -> TestResult {
    let sql = r#"
        WITH line(bucket,x) AS (
            VALUES (1,-5),(1,4),(1,8),(2,10),(2,12)
        )
        SELECT CAST(SUM(bucket*(high-low)) AS BIGINT)
        FROM (
            SELECT bucket, MAX(x) AS high, MIN(x) AS low
            FROM line GROUP BY bucket
        ) bounds
    "#;
    assert_query(sql, 17).await
}
