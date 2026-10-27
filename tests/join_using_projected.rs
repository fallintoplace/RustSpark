use datafusion::logical_expr::JoinType;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn projected_using_returns_only_requested_columns() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view(
        "order",
        "SELECT 1 AS id, 'east' AS region, 10 AS amount, 'left' AS hidden
         UNION ALL SELECT 2, 'west', 20, 'left2'",
    ).await?;
    session.register_sql_view(
        "lookup",
        "SELECT 1 AS id, 'east' AS region, 'known' AS label, 'right' AS hidden
         UNION ALL SELECT 3, 'north', 'other', 'right3'",
    ).await?;

    let joined = session
        .join_tables_using_projected(
            "order", "lookup", JoinType::Full, &["id", "region"], &["amount"], &["label"],
        ).await?;
    let columns = joined.schema().fields().iter()
        .map(|field| field.name().to_string()).collect::<Vec<_>>();
    assert_eq!(columns, vec!["id", "region", "amount", "label"]);
    assert_eq!(joined.count().await?, 3);
    Ok(())
}

#[tokio::test]
async fn projected_using_rejects_missing_or_colliding_columns() -> TestResult {
    let session = RustSparkSession::new();
    session.register_sql_view("l", "SELECT 1 AS id, 'left' AS hidden").await?;
    session.register_sql_view("r", "SELECT 1 AS id, 'right' AS hidden").await?;
    assert!(
        session.join_tables_using_projected("l","r", JoinType::Inner, &[], &[], &[])
            .await.is_err()
    );
    assert!(
        session.join_tables_using_projected("l","r", JoinType::Inner, &["id"], &["hidden"], &["hidden"])
            .await.is_err()
    );
    assert!(
        session.join_tables_using_projected("l","r", JoinType::Inner, &["id"], &["id"], &[])
            .await.is_err()
    );
    assert!(
        session.join_tables_using_projected("l","r", JoinType::Inner, &["missing"], &[], &[])
            .await.is_err()
    );
    Ok(())
}
