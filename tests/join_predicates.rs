use datafusion::{
    logical_expr::{Expr, JoinType},
    prelude::{col, lit},
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn predicate_session() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "points",
            "SELECT CAST(1 AS BIGINT) AS point
             UNION ALL SELECT 3
             UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    session
        .register_sql_view(
            "limits",
            "SELECT CAST(2 AS BIGINT) AS upper_bound
             UNION ALL SELECT 4",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn non_equality_joins_preserve_sql_null_and_outer_semantics() -> TestResult {
    let session = predicate_session().await?;
    for (kind, expected) in [
        (JoinType::Inner, 3),
        (JoinType::Left, 4),
        (JoinType::LeftSemi, 2),
        (JoinType::LeftAnti, 1),
        (JoinType::RightSemi, 2),
        (JoinType::RightAnti, 0),
    ] {
        let count = session
            .join_tables_on(
                "points",
                "limits",
                kind,
                [col("point").lt(col("upper_bound"))],
            )
            .await?
            .count()
            .await?;
        assert_eq!(count, expected, "{kind:?}");
    }
    Ok(())
}

#[tokio::test]
async fn multiple_predicates_are_conjoined() -> TestResult {
    let session = predicate_session().await?;
    let matches = session
        .join_tables_on(
            "points",
            "limits",
            JoinType::Inner,
            [col("point").lt(col("upper_bound")), col("point").gt(lit(1))],
        )
        .await?
        .count()
        .await?;
    assert_eq!(matches, 1);

    // Equality expressions also work without requiring a separate API.
    let equality_matches = session
        .join_tables_on(
            "points",
            "limits",
            JoinType::Inner,
            [col("point").eq(col("upper_bound"))],
        )
        .await?
        .count()
        .await?;
    assert_eq!(equality_matches, 0);
    Ok(())
}

#[tokio::test]
async fn empty_predicates_and_missing_columns_fail_during_planning() -> TestResult {
    let session = predicate_session().await?;
    assert!(
        session
            .join_tables_on("points", "limits", JoinType::Inner, Vec::<Expr>::new())
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on(
                "points",
                "limits",
                JoinType::Inner,
                [col("unknown").eq(col("upper_bound"))],
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on(
                "missing",
                "limits",
                JoinType::Inner,
                [col("point").eq(col("upper_bound"))],
            )
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_on(
                "points",
                "missing",
                JoinType::Inner,
                [col("point").eq(col("upper_bound"))],
            )
            .await
            .is_err()
    );
    Ok(())
}
