use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[tokio::test]
async fn registered_sql_views_reject_schema_mutations() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let ddl = "CREATE TABLE forbidden AS SELECT 1 AS n";
    assert!(session.register_sql_view("lazy_ddl", ddl).await.is_err());
    assert!(session.register_materialized_sql_view("cached_ddl", ddl).await.is_err());
    assert!(
        session.register_sql_view_with_params("param_ddl", ddl, Vec::<ScalarValue>::new())
            .await.is_err()
    );
    assert!(
        session.register_materialized_sql_view_with_params(
            "param_cached_ddl", ddl, Vec::<ScalarValue>::new()
        ).await.is_err()
    );
    assert!(!session.table_exists("forbidden")?);
    for name in ["lazy_ddl", "cached_ddl", "param_ddl", "param_cached_ddl"] {
        assert!(!session.table_exists(name)?);
    }

    session.register_sql_view("valid", "SELECT 42 AS n").await?;
    assert_eq!(session.table("valid").await?.count().await?, 1);
    Ok(())
}
