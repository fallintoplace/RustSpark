use datafusion::{
    logical_expr::JoinType,
    prelude::{col, lit},
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn filtered_join_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "events",
            "SELECT CAST(1 AS BIGINT) AS account, CAST(9 AS BIGINT) AS score
             UNION ALL SELECT 1, 3
             UNION ALL SELECT 2, 4",
        )
        .await?;
    session
        .register_sql_view(
            "thresholds",
            "SELECT CAST(1 AS BIGINT) AS account_id, CAST(5 AS BIGINT) AS minimum
             UNION ALL SELECT 1, 10
             UNION ALL SELECT 2, 3",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn join_filter_limits_matches_without_dropping_outer_rows() -> TestResult {
    let session = filtered_join_session().await?;
    let keys = &[("account", "account_id")];

    assert_eq!(
        session
            .join_tables("events", "thresholds", JoinType::Inner, keys)
            .await?
            .count()
            .await?,
        5
    );
    for (join_type, expected) in [
        (JoinType::Inner, 2),
        (JoinType::Left, 3),
        (JoinType::LeftSemi, 2),
        (JoinType::LeftAnti, 1),
    ] {
        let count = session
            .join_tables_filtered(
                "events",
                "thresholds",
                join_type,
                keys,
                col("score").gt_eq(col("minimum")),
            )
            .await?
            .count()
            .await?;
        assert_eq!(count, expected, "{join_type:?}");
    }
    Ok(())
}

#[tokio::test]
async fn join_filter_checks_keys_and_predicate_columns() -> TestResult {
    let session = filtered_join_session().await?;
    assert!(
        session
            .join_tables_filtered("events", "thresholds", JoinType::Inner, &[], lit(true))
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_filtered(
                "events",
                "thresholds",
                JoinType::Inner,
                &[("account", "")],
                lit(true),
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_filtered(
                "events",
                "thresholds",
                JoinType::Inner,
                &[("account", "account_id")],
                col("unknown").gt(lit(1)),
            )
            .await
            .is_err()
    );
    Ok(())
}
