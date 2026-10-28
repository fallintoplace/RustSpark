use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn null_safe_using_matches_null_keys_and_coalesces_the_output() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS left_value
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null-left'
             UNION ALL SELECT 2, 'b'",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS right_value
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null-right'
             UNION ALL SELECT 3, 'c'",
        )
        .await?;

    assert_eq!(
        session
            .join_tables_using("order", "lookup", JoinType::Inner, &["id"])
            .await?
            .count()
            .await?,
        1
    );
    for (kind, expected) in [
        (JoinType::Inner, 2),
        (JoinType::Left, 3),
        (JoinType::Right, 3),
        (JoinType::Full, 4),
    ] {
        let result = session
            .join_tables_using_null_safe("order", "lookup", kind, &["id"])
            .await?;
        let key_count = result
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() == "id")
            .count();
        assert_eq!(key_count, 1);
        assert_eq!(result.count().await?, expected, "{kind:?}");
    }
    Ok(())
}

#[tokio::test]
async fn null_safe_using_checks_keys_and_join_type() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view("l", "SELECT 1 AS id").await?;
    session.register_sql_view("r", "SELECT 1 AS id").await?;
    assert!(
        session
            .join_tables_using_null_safe("l", "r", JoinType::Inner, &[])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using_null_safe("l", "r", JoinType::Inner, &["id", "ID"])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using_null_safe("l", "r", JoinType::LeftAnti, &["id"])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using_null_safe("l", "r", JoinType::Inner, &["missing"])
            .await
            .is_err()
    );
    Ok(())
}
