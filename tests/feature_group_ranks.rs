use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn group_ranks_preserve_ties_and_choose_dense_behavior() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 'a' AS team, 10 AS points UNION ALL SELECT 'a', 10 UNION ALL
             SELECT 'a', 5 UNION ALL SELECT 'b', 9",
        )
        .await?;
    session.register_dataframe(
        "ranked",
        session
            .with_group_ranks("scores", &["team"], &[("points", false)], "rank id", false)
            .await?,
    )?;
    session.register_dataframe(
        "dense_ranked",
        session
            .with_group_ranks("scores", &["team"], &[("points", false)], "rank id", true)
            .await?,
    )?;
    assert_eq!(
        session
            .sql_scalar("SELECT \"rank id\" FROM ranked WHERE team = 'a' AND points = 5")
            .await?
            .to_string(),
        "3"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT \"rank id\" FROM dense_ranked WHERE team = 'a' AND points = 5")
            .await?
            .to_string(),
        "2"
    );
    assert_eq!(
        session
            .sql_scalar("SELECT \"rank id\" FROM ranked WHERE team = 'b'")
            .await?
            .to_string(),
        "1"
    );
    assert!(
        session
            .with_group_ranks("scores", &[], &[("points", false)], "rank", true)
            .await
            .is_err()
    );
    assert!(
        session
            .with_group_ranks("scores", &["team"], &[("points", false)], "team", false)
            .await
            .is_err()
    );
    Ok(())
}
