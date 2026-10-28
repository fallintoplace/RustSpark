use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn self_pairs_keep_each_unordered_group_pair_once() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT 1 AS basket, 'east' AS region, 1 AS item
             UNION ALL SELECT 1, 'east', 2
             UNION ALL SELECT 1, 'east', 3
             UNION ALL SELECT 1, 'west', 4
             UNION ALL SELECT 1, 'west', 5
             UNION ALL SELECT 2, 'east', 6
             UNION ALL SELECT CAST(NULL AS INT), 'east', 7",
        )
        .await?;

    let pairs = session
        .join_table_distinct_pairs("order", &["basket", "region"], "item")
        .await?;
    assert_eq!(pairs.count().await?, 4);
    assert_eq!(
        session
            .join_table_distinct_pairs("order", &["basket"], "item")
            .await?
            .count()
            .await?,
        10
    );
    Ok(())
}

#[tokio::test]
async fn self_pairs_validate_keys_before_planning() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("items", "SELECT 1 AS basket, 1 AS item")
        .await?;
    assert!(session.join_table_distinct_pairs("items", &[], "item").await.is_err());
    assert!(
        session
            .join_table_distinct_pairs("items", &["basket", "BASKET"], "item")
            .await
            .is_err()
    );
    assert!(
        session
            .join_table_distinct_pairs("items", &["item"], "item")
            .await
            .is_err()
    );
    assert!(
        session
            .join_table_distinct_pairs("items", &["basket"], "missing")
            .await
            .is_err()
    );
    Ok(())
}
