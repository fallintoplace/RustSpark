use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn prefix_join_treats_wildcards_as_literal_characters() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT 'Amsterdam' AS text_value
             UNION ALL SELECT 'Berlin'
             UNION ALL SELECT '100% done'
             UNION ALL SELECT CAST(NULL AS VARCHAR)",
        )
        .await?;
    session
        .register_sql_view(
            "prefixes",
            "SELECT 'Am' AS prefix
             UNION ALL SELECT 'Ber'
             UNION ALL SELECT '100%'",
        )
        .await?;
    for (join_type, expected) in [(JoinType::Inner, 3), (JoinType::Left, 4)] {
        let count = session
            .join_tables_on_prefix("order", "prefixes", join_type, "text_value", "prefix")
            .await?
            .count()
            .await?;
        assert_eq!(count, expected, "{join_type:?}");
    }
    Ok(())
}

#[tokio::test]
async fn prefix_join_validates_inputs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("l", "SELECT 'text' AS value")
        .await?;
    session
        .register_sql_view("r", "SELECT 't' AS prefix")
        .await?;
    assert!(
        session
            .join_tables_on_prefix("l", "r", JoinType::Full, "value", "prefix")
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_prefix("l", "r", JoinType::Inner, "missing", "prefix")
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_prefix("l", "r", JoinType::Inner, "value", " ")
            .await
            .is_err()
    );
    Ok(())
}
