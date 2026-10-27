use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn assert_scalar(sql: &str, expected: i64) -> TestResult {
    let actual = RustSparkSession::new().sql_scalar(sql).await?;
    assert_eq!(actual, ScalarValue::Int64(Some(expected)), "{sql}");
    Ok(())
}

#[tokio::test]
async fn range_window_includes_peers_at_same_key() -> TestResult {
    let sql = r#"
        WITH readings(ts,v) AS (VALUES (1,2),(2,3),(2,7),(3,11))
         SELECT COUNT(*) FROM (SELECT ts,v,
         SUM(v) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
         FROM readings) x WHERE ts=2 AND running=12
    "#;
    assert_scalar(sql, 2).await
}

#[tokio::test]
async fn last_value_full_frame_is_partition_final() -> TestResult {
    let sql = r#"
        WITH scores(g,seq,v) AS (VALUES ('A',1,2),('A',2,7),('A',3,9),('B',1,3),('B',2,10))
         SELECT COUNT(*) FROM (SELECT g,seq,v,
         LAST_VALUE(v) OVER (PARTITION BY g ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_value
         FROM scores) s WHERE (g='A' AND final_value=9) OR (g='B' AND final_value=10)
    "#;
    assert_scalar(sql, 5).await
}

#[tokio::test]
async fn lead_default_stays_inside_partition() -> TestResult {
    let sql = r#"
        WITH series(g,seq,v) AS (VALUES ('A',1,2),('A',2,5),('B',1,7),('B',2,11))
         SELECT COUNT(*) FROM (SELECT g,seq,v,
         LEAD(v,1,-1) OVER (PARTITION BY g ORDER BY seq) AS next_value FROM series) x
         WHERE (seq=2 AND next_value=-1) OR (g='A' AND seq=1 AND next_value=5)
    "#;
    assert_scalar(sql, 3).await
}

#[tokio::test]
async fn window_rank_respects_explicit_null_position() -> TestResult {
    let sql = r#"
        WITH scores(id,points) AS (VALUES (1,8),(2,NULL),(3,10),(4,8))
         SELECT COUNT(*) FROM (SELECT id,
         ROW_NUMBER() OVER (ORDER BY points DESC NULLS LAST,id) AS rn FROM scores) s
         WHERE (id=3 AND rn=1) OR (id=2 AND rn=4)
    "#;
    assert_scalar(sql, 2).await
}
