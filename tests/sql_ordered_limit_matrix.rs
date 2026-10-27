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
async fn top_k_uses_unique_tiebreaker_before_limit() -> TestResult {
    let sql = r#"
        WITH items(id,score) AS (
            VALUES (1,10),(2,10),(3,20),(4,20),(5,5)
        )
        SELECT CAST(SUM(id) AS BIGINT)
        FROM (
            SELECT id FROM items ORDER BY score DESC, id ASC LIMIT 3
        ) top_items
    "#;
    assert_query(sql, 8).await
}

#[tokio::test]
async fn limit_offset_applies_after_multi_key_sort() -> TestResult {
    let sql = r#"
        WITH items(id,score) AS (
            VALUES (1,3),(2,3),(3,5),(4,7),(5,7)
        )
        SELECT CAST(SUM(id) AS BIGINT)
        FROM (
            SELECT id FROM items
            ORDER BY score ASC, id DESC LIMIT 2 OFFSET 1
        ) page
    "#;
    assert_query(sql, 4).await
}

#[tokio::test]
async fn group_top_k_orders_after_having() -> TestResult {
    let sql = r#"
        WITH items(grp,x) AS (
            VALUES (1,4),(1,6),(2,20),(3,5),(3,5),(4,7)
        )
        SELECT CAST(SUM(grp*100+total) AS BIGINT)
        FROM (
            SELECT grp, SUM(x) AS total FROM items
            GROUP BY grp HAVING SUM(x)>8
            ORDER BY total DESC, grp DESC LIMIT 2
        ) top_groups
    "#;
    assert_query(sql, 530).await
}

#[tokio::test]
async fn distinct_values_are_selected_before_top_k() -> TestResult {
    let sql = r#"
        WITH items(id,score) AS (
            VALUES (1,10),(2,10),(3,30),(4,20),(5,20)
        )
        SELECT CAST(SUM(score) AS BIGINT)
        FROM (
            SELECT DISTINCT score FROM items ORDER BY score DESC LIMIT 2
        ) top_scores
    "#;
    assert_query(sql, 50).await
}
