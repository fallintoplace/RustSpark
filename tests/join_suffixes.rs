use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn colliding_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT 1 AS id, 'A' AS status, 'left' AS note
             UNION ALL SELECT 2, 'B', 'left2'",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT 1 AS id, 'READY' AS status, 'right' AS info
             UNION ALL SELECT 3, 'WAIT', 'right3'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn shared_join_column_names_get_unique_suffixes() -> TestResult {
    let session = colliding_session().await?;
    for (kind, expected) in [(JoinType::Inner, 1), (JoinType::Left, 2), (JoinType::Full, 3)] {
        let joined = session
            .join_tables_with_suffixes("order", "lookup", kind, &[("id", "id")], "_l", "_r")
            .await?;
        let columns: Vec<_> = joined
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str().to_owned())
            .collect();
        assert_eq!(
            columns,
            vec!["id_l", "status_l", "note", "id_r", "status_r", "info"]
        );
        assert_eq!(joined.count().await?, expected, "{kind:?}");
    }
    Ok(())
}

#[tokio::test]
async fn suffixes_are_sql_quoted_and_collisions_are_rejected() -> TestResult {
    let session = colliding_session().await?;
    let joined = session
        .join_tables_with_suffixes(
            "order",
            "lookup",
            JoinType::Inner,
            &[("id", "id")],
            "_l\"quoted",
            "_r",
        )
        .await?;
    assert!(joined.schema().fields().iter().any(|field| field.name() == "id_l\"quoted"));
    assert_eq!(joined.count().await?, 1);

    assert!(
        session
            .join_tables_with_suffixes(
                "order",
                "lookup",
                JoinType::Inner,
                &[("id", "id")],
                "_same",
                "_same",
            )
            .await
            .is_err()
    );
    session
        .register_sql_view(
            "left_collision",
            "SELECT 1 AS id, 'a' AS status, 'pre-existing' AS status_l",
        )
        .await?;
    assert!(
        session
            .join_tables_with_suffixes(
                "left_collision",
                "lookup",
                JoinType::Inner,
                &[("id", "id")],
                "_l",
                "_r",
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn suffix_join_rejects_invalid_arguments() -> TestResult {
    let session = colliding_session().await?;
    assert!(
        session
            .join_tables_with_suffixes("order", "lookup", JoinType::Inner, &[], "_l", "_r")
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_with_suffixes(
                "order",
                "lookup",
                JoinType::LeftSemi,
                &[("id", "id")],
                "_l",
                "_r",
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_with_suffixes(
                "order",
                "lookup",
                JoinType::Inner,
                &[("id", "unknown")],
                "_l",
                "_r",
            )
            .await
            .is_err()
    );
    Ok(())
}
