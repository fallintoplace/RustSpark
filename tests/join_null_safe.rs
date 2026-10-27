use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn join_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "left_keys",
            "SELECT CAST(1 AS BIGINT) AS id, 'one' AS left_value
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null'
             UNION ALL SELECT 2, 'two'",
        )
        .await?;
    session
        .register_sql_view(
            "right_keys",
            "SELECT CAST(1 AS BIGINT) AS other_id, 'one' AS right_value
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null'
             UNION ALL SELECT 3, 'three'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn null_safe_keys_match_nulls_without_matching_non_nulls() -> TestResult {
    let session = join_session().await?;
    let keys = &[("id", "other_id")];
    assert_eq!(
        session
            .join_tables("left_keys", "right_keys", JoinType::Inner, keys)
            .await?
            .count()
            .await?,
        1
    );
    for (join_type, expected) in [
        (JoinType::Inner, 2),
        (JoinType::Left, 3),
        (JoinType::Right, 3),
        (JoinType::Full, 4),
    ] {
        assert_eq!(
            session
                .join_tables_null_safe("left_keys", "right_keys", join_type, keys)
                .await?
                .count()
                .await?,
            expected,
            "{join_type:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn null_safe_join_requires_real_columns_and_keys() -> TestResult {
    let session = join_session().await?;
    assert!(
        session
            .join_tables_null_safe("left_keys", "right_keys", JoinType::Inner, &[])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_null_safe("left_keys", "right_keys", JoinType::Inner, &[("id", " ")])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_null_safe(
                "left_keys",
                "right_keys",
                JoinType::LeftSemi,
                &[("id", "other_id")]
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_null_safe(
                "left_keys",
                "right_keys",
                JoinType::Inner,
                &[("bad", "other_id")]
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn null_safe_join_quotes_reserved_table_names() -> TestResult {
    let session = join_session().await?;
    session
        .register_sql_view("order", "SELECT CAST(NULL AS BIGINT) AS id")
        .await?;
    let rows = session
        .join_tables_null_safe(
            "order",
            "right_keys",
            JoinType::Inner,
            &[("id", "other_id")],
        )
        .await?
        .count()
        .await?;
    assert_eq!(rows, 1);
    Ok(())
}
