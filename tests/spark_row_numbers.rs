use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn row_numbers_follow_sort_direction_and_null_order() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "items",
            "SELECT CAST(NULL AS BIGINT) AS id UNION ALL SELECT 1 UNION ALL SELECT 3",
        )
        .await?;

    let ascending = session
        .with_row_numbers("items", "ordinal", &[("id", true)])
        .await?;
    session.register_dataframe("ascending", ascending)?;
    assert_eq!(
        session
            .sql_scalar("SELECT ordinal FROM ascending WHERE id IS NULL")
            .await?
            .to_string(),
        "1"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT ordinal FROM ascending WHERE id = 3")
            .await?
            .to_string(),
        "3"
    );

    let descending = session
        .with_row_numbers("items", "ordinal", &[("id", false)])
        .await?;
    session.register_dataframe("descending", descending)?;
    assert_eq!(
        session
            .sql_scalar("SELECT ordinal FROM descending WHERE id = 3")
            .await?
            .to_string(),
        "1"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT ordinal FROM descending WHERE id IS NULL")
            .await?
            .to_string(),
        "3"
    );
    assert_eq!(session.table("items").await?.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn row_numbers_validate_names_and_order_keys() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view("items", "SELECT 1 AS id").await?;

    assert!(
        session
            .with_row_numbers("items", "id", &[("id", true)])
            .await
            .is_err()
    );
    assert!(
        session
            .with_row_numbers("items", "ID", &[("id", true)])
            .await
            .is_err()
    );
    assert!(
        session
            .with_row_numbers("items", " padded ", &[("id", true)])
            .await
            .is_err()
    );
    assert!(
        session
            .with_row_numbers("items", "ordinal", &[])
            .await
            .is_err()
    );
    assert!(
        session
            .with_row_numbers("items", "ordinal", &[("missing", true)])
            .await
            .is_err()
    );
    assert!(
        session
            .with_row_numbers("items", "ordinal", &[("id", true), ("ID", false)])
            .await
            .is_err()
    );

    let quoted = session
        .with_row_numbers("items", "row \"number", &[("id", true)])
        .await?;
    assert!(
        quoted
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == "row \"number")
    );
    Ok(())
}
