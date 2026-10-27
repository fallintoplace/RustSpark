use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn session_with_shared_keys() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT 1 AS id, 'east' AS region, 'A' AS left_value
             UNION ALL SELECT 2, 'west', 'B'",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT 1 AS id, 'east' AS region, 'ok' AS right_value
             UNION ALL SELECT 2, 'east', 'different-region'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn shared_column_join_uses_all_matching_names() -> TestResult {
    let session = session_with_shared_keys().await?;
    for (join_type, expected) in [
        (JoinType::Inner, 1),
        (JoinType::Left, 2),
        (JoinType::Right, 2),
        (JoinType::Full, 3),
    ] {
        let joined = session
            .join_tables_on_common_columns("order", "lookup", join_type)
            .await?;
        let names: Vec<_> = joined
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        assert_eq!(
            names,
            vec!["id", "region", "left_value", "right_value"],
            "{join_type:?}"
        );
        assert_eq!(joined.count().await?, expected, "{join_type:?}");
    }
    Ok(())
}

#[tokio::test]
async fn shared_column_join_rejects_unrelated_tables() -> TestResult {
    let session = session_with_shared_keys().await?;
    session
        .register_sql_view("unrelated", "SELECT 1 AS other")
        .await?;
    assert!(
        session
            .join_tables_on_common_columns("order", "unrelated", JoinType::Inner)
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_common_columns("missing", "lookup", JoinType::Inner)
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on_common_columns("order", "lookup", JoinType::LeftSemi)
            .await
            .is_err()
    );
    Ok(())
}
