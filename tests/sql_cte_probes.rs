use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn reuse_aggregate_cte_in_join_and_filter() -> TestResult {
    let sql = r#"
        WITH raw(k,v) AS (VALUES ('A',1),('A',4),('B',3)),
         totals AS (SELECT k,SUM(v) AS total FROM raw GROUP BY k),
         good AS (SELECT k FROM totals WHERE total >= 3)
         SELECT COUNT(*) FROM good g JOIN totals t ON t.k=g.k
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn derived_table_alias_stays_addressable() -> TestResult {
    let sql = r#"
        WITH source(id,v) AS (VALUES (1,6),(2,7),(3,3))
         SELECT COUNT(*) FROM (SELECT id,v*2 AS doubled FROM source) d
         WHERE d.doubled > 10 AND d.id <> 2
    "#;
    assert_scalar(sql, 1).await
}

#[tokio::test]
async fn distinct_after_union_all_deduplicates() -> TestResult {
    let sql = r#"
        WITH left_ids(id) AS (VALUES (1),(2),(2)),
         right_ids(id) AS (VALUES (2),(3),(4)),
         all_ids AS (SELECT id FROM left_ids UNION ALL SELECT id FROM right_ids)
         SELECT COUNT(*) FROM (SELECT DISTINCT id FROM all_ids) d
    "#;
    assert_scalar(sql, 4).await
}

#[tokio::test]
async fn filter_after_grouped_cte_join() -> TestResult {
    let sql = r#"
        WITH items(g,v) AS (VALUES ('A',2),('A',4),('B',2),('C',9)),
         thresholds(g,minimum) AS (VALUES ('A',5),('B',3),('C',8)),
         totals AS (SELECT g,SUM(v) AS total FROM items GROUP BY g)
         SELECT COUNT(*) FROM totals t JOIN thresholds l ON t.g=l.g WHERE t.total>l.minimum
    "#;
    assert_scalar(sql, 2).await
}
