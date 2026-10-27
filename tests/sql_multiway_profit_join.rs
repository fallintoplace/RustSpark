use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn multiway_join_aggregates_filtered_profit_without_row_multiplication()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("part",
        "SELECT 1 AS partkey, 'green steel' AS name UNION ALL SELECT 2, 'blue steel'").await?;
    session.register_sql_view("supplier",
        "SELECT 1 AS suppkey, 10 AS nationkey UNION ALL SELECT 2, 20").await?;
    session.register_sql_view("nation",
        "SELECT 10 AS nationkey, 'East' AS name UNION ALL SELECT 20, 'West'").await?;
    session.register_sql_view("orders",
        "SELECT 100 AS orderkey UNION ALL SELECT 200").await?;
    session.register_sql_view("partsupp",
        "SELECT 1 AS partkey, 1 AS suppkey, 2 AS supplycost
         UNION ALL SELECT 1, 2, 4 UNION ALL SELECT 2, 1, 1").await?;
    session.register_sql_view("lineitem",
        "SELECT 100 AS orderkey, 1 AS partkey, 1 AS suppkey, 5 AS qty, 50 AS price
         UNION ALL SELECT 200, 1, 2, 2, 30
         UNION ALL SELECT 200, 2, 1, 3, 30").await?;
    let total = session.sql_scalar(
        "SELECT SUM(profit) FROM (
             SELECT n.name, SUM(l.price - ps.supplycost * l.qty) AS profit
             FROM part p
             JOIN lineitem l ON p.partkey = l.partkey
             JOIN orders o ON o.orderkey = l.orderkey
             JOIN supplier s ON s.suppkey = l.suppkey
             JOIN partsupp ps ON ps.partkey = l.partkey AND ps.suppkey = l.suppkey
             JOIN nation n ON n.nationkey = s.nationkey
             WHERE p.name LIKE 'green%'
             GROUP BY n.name
         ) profits",
    ).await?;
    assert_eq!(total, ScalarValue::Int64(Some(62)));
    Ok(())
}
