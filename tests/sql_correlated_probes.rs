use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn correlated_avg_compares_within_each_team() -> TestResult {
    let sql = r#"
        WITH scores(team, person, points) AS (
         VALUES ('A',1,3),('A',2,7),('B',3,4),('B',4,9))
         SELECT COUNT(*) FROM scores s WHERE s.points >
         (SELECT AVG(t.points) FROM scores t WHERE t.team = s.team)
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn correlated_max_defaults_for_missing_rows() -> TestResult {
    let sql = r#"
        WITH accounts(id) AS (VALUES (1),(2),(3),(4)),
         events(account_id,amount) AS (VALUES (1,8),(1,2),(2,3),(3,NULL))
         SELECT COUNT(*) FROM accounts a WHERE COALESCE(
         (SELECT MAX(e.amount) FROM events e WHERE e.account_id=a.id),0) >= 3
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn correlated_exists_applies_per_parent_threshold() -> TestResult {
    let sql = r#"
        WITH orders(id,min_amount) AS (VALUES (1,6),(2,3),(3,9)),
         lines(order_id,amount) AS (VALUES (1,8),(1,1),(2,2),(2,1),(3,10))
         SELECT COUNT(*) FROM orders o WHERE EXISTS (
         SELECT 1 FROM lines l WHERE l.order_id=o.id AND l.amount>o.min_amount)
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn correlated_count_distinct_uses_matching_children() -> TestResult {
    let sql = r#"
        WITH users(id) AS (VALUES (1),(2),(3),(4)),
         visits(user_id,device) AS (VALUES (1,'web'),(1,'mobile'),(2,'web'),(3,'web'),(3,'web'))
         SELECT COUNT(*) FROM users u WHERE (
         SELECT COUNT(DISTINCT v.device) FROM visits v WHERE v.user_id=u.id)>=2
    "#;
    assert_scalar(sql, 1).await
}
