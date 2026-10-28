use datafusion::logical_expr::JoinType;
use rustspark::{MultiJoinStep, RustSparkSession};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> TestResultSession {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "orders",
            "SELECT 1 AS buyer_id, 10 AS product_id
             UNION ALL SELECT 2, 20
             UNION ALL SELECT 3, 30",
        )
        .await?;
    session
        .register_sql_view(
            "users",
            "SELECT 1 AS id, 'Ada' AS label UNION ALL SELECT 3, 'Cam'",
        )
        .await?;
    session
        .register_sql_view(
            "products",
            "SELECT 10 AS id, 'A' AS sku UNION ALL SELECT 20, 'B'",
        )
        .await?;
    Ok(session)
}

type TestResultSession = Result<RustSparkSession, Box<dyn std::error::Error>>;

#[tokio::test]
async fn joins_three_tables_with_independent_fact_keys() -> TestResult {
    let session = fixture().await?;
    let result = session
        .join_many_tables(
            "orders",
            "o",
            &[
                MultiJoinStep {
                    table: "users",
                    alias: "u",
                    join_type: JoinType::Left,
                    keys: &[("o", "buyer_id", "id")],
                },
                MultiJoinStep {
                    table: "products",
                    alias: "p",
                    join_type: JoinType::Inner,
                    keys: &[("o", "product_id", "id")],
                },
            ],
        )
        .await?;
    assert_eq!(result.count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn joins_can_reference_an_earlier_dimension_and_repeat_tables() -> TestResult {
    let session = fixture().await?;
    let result = session
        .join_many_tables(
            "users",
            "u",
            &[
                MultiJoinStep {
                    table: "users",
                    alias: "peer",
                    join_type: JoinType::Inner,
                    keys: &[("u", "id", "id")],
                },
                MultiJoinStep {
                    table: "orders",
                    alias: "o",
                    join_type: JoinType::Left,
                    keys: &[("peer", "id", "buyer_id")],
                },
            ],
        )
        .await?;
    assert_eq!(result.count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn rejects_ambiguous_aliases_and_empty_or_future_keys() -> TestResult {
    let session = fixture().await?;
    assert!(session.join_many_tables("orders", "o", &[]).await.is_err());
    for steps in [
        vec![MultiJoinStep {
            table: "users",
            alias: "O",
            join_type: JoinType::Inner,
            keys: &[("o", "buyer_id", "id")],
        }],
        vec![MultiJoinStep {
            table: "users",
            alias: "u",
            join_type: JoinType::Inner,
            keys: &[("future", "buyer_id", "id")],
        }],
        vec![MultiJoinStep {
            table: "users",
            alias: "u",
            join_type: JoinType::Inner,
            keys: &[],
        }],
        vec![MultiJoinStep {
            table: "users",
            alias: "u",
            join_type: JoinType::LeftSemi,
            keys: &[("o", "buyer_id", "id")],
        }],
    ] {
        assert!(
            session
                .join_many_tables("orders", "o", &steps)
                .await
                .is_err()
        );
    }
    Ok(())
}
