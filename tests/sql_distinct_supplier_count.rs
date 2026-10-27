use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn distinct_supplier_count_filters_excluded_suppliers()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("part",
        "SELECT 1 AS partkey, 'Brand-A' AS brand
         UNION ALL SELECT 2, 'Brand-A'
         UNION ALL SELECT 3, 'Brand-B'").await?;
    session.register_sql_view("partsupp",
        "SELECT 1 AS partkey, 1 AS suppkey
         UNION ALL SELECT 1, 2
         UNION ALL SELECT 2, 1
         UNION ALL SELECT 2, 3
         UNION ALL SELECT 3, 2").await?;
    session.register_sql_view("supplier",
        "SELECT 1 AS suppkey, 'normal' AS comment
         UNION ALL SELECT 2, 'normal'
         UNION ALL SELECT 3, 'Customer Complaints'").await?;
    let distinct = session.sql_scalar(
        "SELECT COUNT(DISTINCT ps.suppkey) FROM part p
         JOIN partsupp ps ON p.partkey = ps.partkey
         WHERE p.brand = 'Brand-A'
           AND ps.suppkey NOT IN (
               SELECT s.suppkey FROM supplier s
               WHERE s.comment LIKE '%Customer%Complaints%'
           )",
    ).await?;
    assert_eq!(distinct, ScalarValue::Int64(Some(2)));
    Ok(())
}
