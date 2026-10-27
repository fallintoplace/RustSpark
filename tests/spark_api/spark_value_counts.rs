use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn value_counts_groups_values_including_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT 'apple' AS \"label value\"
             UNION ALL SELECT 'apple'
             UNION ALL SELECT 'pear'
             UNION ALL SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;

    let frequencies = session.value_counts("items", "label value").await?;
    session.register_dataframe("frequencies", frequencies)?;
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM frequencies WHERE value = 'apple'")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM frequencies WHERE value = 'pear'")
            .await?
            .to_string(),
        "1"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM frequencies WHERE value IS NULL")
            .await?
            .to_string(),
        "1"
    );
    assert_eq!(session.table("items").await?.count().await?, 4);

    let qualified = session
        .value_counts("datafusion.public.items", "label value")
        .await?;
    assert_eq!(qualified.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn value_counts_quotes_column_names_and_rejects_unknown_fields() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "special",
            "SELECT 1 AS \"odd\"\"column\" UNION ALL SELECT 1",
        )
        .await?;
    let frequencies = session.value_counts("special", "odd\"column").await?;
    session.register_dataframe("freq", frequencies)?;
    assert_eq!(
        session
            .sql_scalar("SELECT frequency FROM freq WHERE value = 1")
            .await?
            .to_string(),
        "2"
    );
    assert!(session.value_counts("special", "missing").await.is_err());
    assert!(
        session
            .value_counts("special", "odd\"column; DROP TABLE special")
            .await
            .is_err()
    );
    assert!(session.table_exists("special")?);
    Ok(())
}
