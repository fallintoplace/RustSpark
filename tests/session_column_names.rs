use rustspark::RustSparkSession;

#[tokio::test]
async fn gets_ordered_column_names_without_collecting_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 1 AS \"odd name\", CAST(NULL AS BIGINT) AS nullable",
        )
        .await?;
    assert_eq!(
        session.column_names("items").await?,
        vec!["odd name".to_string(), "nullable".to_string()]
    );
    assert!(session.column_names("missing").await.is_err());
    Ok(())
}
