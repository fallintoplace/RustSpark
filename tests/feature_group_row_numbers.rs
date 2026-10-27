use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn group_row_numbers_restart_for_each_partition() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "scores",
            "SELECT 'a' AS team, 10 AS score UNION ALL
             SELECT 'a', 20 UNION ALL SELECT 'b', 30",
        )
        .await?;
    let numbered = session
        .with_group_row_numbers("scores", &["team"], &[("score", false)], "rank number")
        .await?;
    session.register_dataframe("numbered", numbered)?;
    assert_eq!(
        session.sql_scalar("SELECT MAX(\"rank number\") FROM numbered WHERE team = 'a'").await?.to_string(),
        "2"
    );
    assert_eq!(
        session.sql_scalar("SELECT MAX(\"rank number\") FROM numbered WHERE team = 'b'").await?.to_string(),
        "1"
    );
    assert!(session.with_group_row_numbers("scores", &[], &[("score", false)], "rn").await.is_err());
    assert!(session.with_group_row_numbers("scores", &["team"], &[], "rn").await.is_err());
    assert!(session.with_group_row_numbers("scores", &["team"], &[("score", false)], "team").await.is_err());
    Ok(())
}
