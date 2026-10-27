use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn intersect_treats_null_as_set_member() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (1),(1),(2),(NULL)),
         b(v) AS (VALUES (1),(NULL),(3))
         SELECT COUNT(*) FROM (SELECT v FROM a INTERSECT SELECT v FROM b) t
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn except_removes_matching_nulls_and_values() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (1),(1),(2),(3),(NULL)),
         b(v) AS (VALUES (1),(NULL),(8))
         SELECT COUNT(*) FROM (SELECT v FROM a EXCEPT SELECT v FROM b) t
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn union_all_preserves_cross_source_duplicates() -> TestResult {
    let sql = r#"
        WITH a(v) AS (VALUES (2),(2),(5)), b(v) AS (VALUES (2),(7))
         SELECT COUNT(*) FROM (SELECT v FROM a UNION ALL SELECT v FROM b) t WHERE v=2
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn full_outer_join_keeps_each_unmatched_side() -> TestResult {
    let sql = r#"
        WITH l(id) AS (VALUES (1),(1),(2),(NULL)),
         r(id) AS (VALUES (1),(3),(NULL))
         SELECT COUNT(*) FROM l FULL OUTER JOIN r ON l.id=r.id
    "#;
    assert_scalar(sql, 6).await
}
