use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn case_insensitive_join_preserves_outer_and_null_semantics() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT 'Alice' AS person, 1 AS left_value
             UNION ALL SELECT 'BOB', 2
             UNION ALL SELECT 'unknown', 3
             UNION ALL SELECT CAST(NULL AS VARCHAR), 4",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT 'alice' AS username, 10 AS right_value
             UNION ALL SELECT 'Bob', 20
             UNION ALL SELECT 'other', 30
             UNION ALL SELECT CAST(NULL AS VARCHAR), 40",
        )
        .await?;

    for (join_type, expected) in [
        (JoinType::Inner, 2),
        (JoinType::Left, 4),
        (JoinType::Right, 4),
        (JoinType::Full, 6),
    ] {
        let joined = session
            .join_tables_case_insensitive("order", "lookup", join_type, &[("person", "username")])
            .await?;
        assert_eq!(joined.count().await?, expected, "{join_type:?}");
    }
    assert_eq!(
        session
            .join_tables(
                "order",
                "lookup",
                JoinType::Inner,
                &[("person", "username")]
            )
            .await?
            .count()
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn case_insensitive_join_rejects_invalid_keys() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("left_rows", "SELECT 'a' AS name")
        .await?;
    session
        .register_sql_view("right_rows", "SELECT 'A' AS name")
        .await?;
    assert!(
        session
            .join_tables_case_insensitive("left_rows", "right_rows", JoinType::Inner, &[])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_case_insensitive(
                "left_rows",
                "right_rows",
                JoinType::LeftSemi,
                &[("name", "name")],
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_case_insensitive(
                "left_rows",
                "right_rows",
                JoinType::Inner,
                &[("missing", "name")],
            )
            .await
            .is_err()
    );
    Ok(())
}
