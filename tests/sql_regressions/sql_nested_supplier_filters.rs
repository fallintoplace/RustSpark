use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn nested_in_with_correlated_sum_filters_each_supplier()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "part",
            "SELECT 1 AS partkey, 'forest pine' AS name UNION ALL SELECT 2, 'urban pine'",
        )
        .await?;
    session.register_sql_view("partsupp", "SELECT 1 AS partkey, 1 AS suppkey, 20 AS availqty UNION ALL SELECT 1, 2, 4 UNION ALL SELECT 2, 3, 80").await?;
    session.register_sql_view("lineitem", "SELECT 1 AS partkey, 1 AS suppkey, 10 AS qty UNION ALL SELECT 1, 1, 10 UNION ALL SELECT 1, 2, 10 UNION ALL SELECT 2, 3, 5").await?;
    session.register_sql_view("supplier", "SELECT 1 AS suppkey, 'Alpha' AS name UNION ALL SELECT 2, 'Beta' UNION ALL SELECT 3, 'Gamma'").await?;
    let count = session
        .sql_scalar(
            "SELECT COUNT(*) FROM supplier s WHERE s.suppkey IN (
             SELECT ps.suppkey FROM partsupp ps
             WHERE ps.partkey IN (SELECT p.partkey FROM part p WHERE p.name LIKE 'forest%')
             AND ps.availqty > (
                 SELECT 0.5 * SUM(l.qty) FROM lineitem l
                 WHERE l.partkey = ps.partkey AND l.suppkey = ps.suppkey
             )
         ) AND s.name = 'Alpha'",
        )
        .await?;
    assert_eq!(count, ScalarValue::Int64(Some(1)));
    Ok(())
}
