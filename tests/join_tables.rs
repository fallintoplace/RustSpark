use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn joined_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "left_rows",
            "SELECT CAST(1 AS BIGINT) AS id, 'us' AS region, 'a' AS tag
             UNION ALL SELECT 2, 'eu', 'b'
             UNION ALL SELECT 2, 'us', 'c'
             UNION ALL SELECT CAST(NULL AS BIGINT), 'us', 'null-left'",
        )
        .await?;
    session
        .register_sql_view(
            "right_rows",
            "SELECT CAST(1 AS BIGINT) AS other_id, 'us' AS area, 'x' AS status
             UNION ALL SELECT 2, 'eu', 'y'
             UNION ALL SELECT 2, 'eu', 'z'
             UNION ALL SELECT 3, 'us', 'unmatched'
             UNION ALL SELECT CAST(NULL AS BIGINT), 'us', 'null-right'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn joins_with_multiple_keys_and_preserves_outer_rows() -> TestResult {
    let session = joined_session().await?;
    for (join_type, expected) in [
        (JoinType::Inner, 3),
        (JoinType::Left, 5),
        (JoinType::Right, 5),
        (JoinType::Full, 7),
    ] {
        let actual = session
            .join_tables(
                "left_rows",
                "right_rows",
                join_type,
                &[("id", "other_id"), ("region", "area")],
            )
            .await?
            .count()
            .await?;
        assert_eq!(actual, expected, "{join_type:?}");
    }
    Ok(())
}

#[tokio::test]
async fn semi_and_anti_joins_keep_each_input_row_at_most_once() -> TestResult {
    let session = joined_session().await?;
    for (join_type, expected) in [
        (JoinType::LeftSemi, 2),
        (JoinType::LeftAnti, 2),
        (JoinType::RightSemi, 3),
        (JoinType::RightAnti, 2),
    ] {
        let actual = session
            .join_tables(
                "left_rows",
                "right_rows",
                join_type,
                &[("id", "other_id"), ("region", "area")],
            )
            .await?
            .count()
            .await?;
        assert_eq!(actual, expected, "{join_type:?}");
    }
    Ok(())
}

#[tokio::test]
async fn single_key_joins_keep_duplicate_matches_without_matching_nulls() -> TestResult {
    let session = joined_session().await?;
    let rows = session
        .join_tables(
            "left_rows",
            "right_rows",
            JoinType::Inner,
            &[("id", "other_id")],
        )
        .await?
        .count()
        .await?;
    assert_eq!(rows, 5);
    Ok(())
}

#[tokio::test]
async fn rejects_empty_keys_and_reports_missing_tables_and_columns() -> TestResult {
    let session = joined_session().await?;
    assert!(
        session
            .join_tables("left_rows", "right_rows", JoinType::Inner, &[])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables("left_rows", "right_rows", JoinType::Inner, &[("id", "")])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables(
                "missing",
                "right_rows",
                JoinType::Inner,
                &[("id", "other_id")]
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables(
                "left_rows",
                "right_rows",
                JoinType::Inner,
                &[("no_id", "other_id")]
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn joining_lazy_table_aliases_preserves_keys_and_rows() -> TestResult {
    let session = joined_session().await?;
    session
        .register_table_alias("left_alias", "left_rows")
        .await?;
    session
        .register_table_alias("right_alias", "right_rows")
        .await?;
    let rows = session
        .join_tables(
            "left_alias",
            "right_alias",
            JoinType::Inner,
            &[("id", "other_id"), ("region", "area")],
        )
        .await?
        .count()
        .await?;
    assert_eq!(rows, 3);
    Ok(())
}
