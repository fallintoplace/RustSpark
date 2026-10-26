use rustspark::RustSparkSession;

#[tokio::test]
async fn set_operations_apply_distinct_and_all_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "left_rows",
            "SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2",
        )
        .await?;
    session
        .register_sql_view(
            "right_rows",
            "SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 3",
        )
        .await?;

    assert_eq!(session.intersect_tables("left_rows", "right_rows").await?.count().await?, 1);
    assert_eq!(session.intersect_tables_all("left_rows", "right_rows").await?.count().await?, 2);
    assert_eq!(session.except_tables("left_rows", "right_rows").await?.count().await?, 1);
    assert_eq!(session.except_tables_all("left_rows", "right_rows").await?.count().await?, 2);

    // Qualified paths behave like plain names.
    assert_eq!(
        session
            .intersect_tables("datafusion.public.left_rows", "datafusion.public.right_rows")
            .await?
            .count()
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn set_operations_reject_bad_table_paths_and_schemas()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view("one", "SELECT 1 AS id").await?;
    session.register_sql_view("two", "SELECT 1 AS id, 2 AS extra").await?;

    assert!(session.except_tables("one", "two").await.is_err());
    assert!(session.intersect_tables("", "one").await.is_err());
    assert!(session.intersect_tables("one..x", "one").await.is_err());
    assert!(
        session
            .except_tables("one; DROP TABLE one", "one")
            .await
            .is_err()
    );
    assert!(session.table_exists("one")?);
    Ok(())
}
