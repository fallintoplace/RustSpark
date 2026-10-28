use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn unpivot_preserves_ids_and_nulls() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "wide",
            "SELECT 1 AS id, 5 AS \"score x\", 'yes' AS \"team's note\" UNION ALL
             SELECT 2, CAST(NULL AS BIGINT), 'no'",
        )
        .await?;
    let long = session
        .unpivot_columns("wide", &["id"], &["score x", "team's note"], "attribute", "value")
        .await?;
    session.register_dataframe("long_form", long)?;
    assert_eq!(session.sql_scalar("SELECT COUNT(*) FROM long_form").await?.to_string(), "4");
    assert_eq!(
        session.sql_scalar("SELECT COUNT(*) FROM long_form WHERE attribute = 'score x' AND value IS NULL").await?.to_string(),
        "1"
    );
    assert_eq!(
        session.sql_scalar("SELECT COUNT(*) FROM long_form WHERE attribute = 'team''s note' AND value = 'yes'").await?.to_string(),
        "1"
    );
    assert!(session.unpivot_columns("wide", &[], &[], "key", "value").await.is_err());
    assert!(session.unpivot_columns("wide", &["id"], &["score x"], "id", "value").await.is_err());
    assert!(session.unpivot_columns("wide", &["id"], &["id"], "key", "value").await.is_err());
    Ok(())
}
