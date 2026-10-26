use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

#[tokio::test]
async fn joins_accept_qualified_catalog_paths_and_reserved_names()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("order", "SELECT 1 AS id").await?;
    session.register_sql_view("group", "SELECT 1 AS id").await?;

    let joined = session
        .join_tables_using(
            "datafusion.public.order",
            "datafusion.public.group",
            JoinType::Inner,
            &["id"],
        )
        .await?;
    assert_eq!(joined.count().await?, 1);
    let cross = session
        .cross_join_tables("datafusion.public.order", "datafusion.public.group")
        .await?;
    assert_eq!(cross.count().await?, 1);
    assert!(
        session
            .join_tables_using("datafusion..order", "group", JoinType::Inner, &["id"],)
            .await
            .is_err()
    );
    Ok(())
}
