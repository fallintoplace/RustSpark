use datafusion::{arrow::array::Int64Array, logical_expr::JoinType, prelude::col};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_sql_view(
        "left_orders", "SELECT CAST(1 AS BIGINT) AS id, 'A' AS orders
                        UNION ALL SELECT 2, 'B'"
    ).await?;
    session.register_sql_view(
        "middle", "SELECT CAST(2 AS BIGINT) AS id, 'M2' AS mid
                   UNION ALL SELECT 3, 'M3'"
    ).await?;
    session.register_sql_view(
        "last", "SELECT CAST(2 AS BIGINT) AS id, 'L2' AS last_value
                 UNION ALL SELECT 4, 'L4'"
    ).await?;
    Ok(session)
}

#[tokio::test]
async fn three_table_inner_left_and_full_joins() -> TestResult {
    let session = fixture().await?;
    for (kind, expected) in [
        (JoinType::Inner, 1),
        (JoinType::Left, 2),
        (JoinType::Full, 4),
    ] {
        let frame = session.join_many_tables_using(
            &["left_orders", "middle", "last"], kind, &["id"],
        ).await?;
        let names: Vec<_> = frame.schema().fields().iter()
            .map(|field| field.name().as_str()).collect();
        assert_eq!(names, ["id", "orders", "mid", "last_value"]);
        assert_eq!(frame.count().await?, expected, "{kind:?}");
    }
    Ok(())
}

#[tokio::test]
async fn full_join_keeps_keys_from_all_three_tables() -> TestResult {
    let session = fixture().await?;
    let frame = session.join_many_tables_using(
        &["left_orders", "middle", "last"], JoinType::Full, &["id"],
    ).await?;
    let batches = frame.sort(vec![col("id").sort(true, true)])?.collect().await?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        ids.extend((0..column.len()).map(|index| column.value(index)));
    }
    assert_eq!(ids, vec![1, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn rejects_missing_keys_and_colliding_payload_names() -> TestResult {
    let session = fixture().await?;
    assert!(session.join_many_tables_using(&["left_orders"], JoinType::Inner, &["id"]).await.is_err());
    assert!(session.join_many_tables_using(&["left_orders", "middle"], JoinType::Inner, &[]).await.is_err());
    assert!(session.join_many_tables_using(&["left_orders", "middle"], JoinType::Inner, &["id", "ID"]).await.is_err());
    assert!(session.join_many_tables_using(&["left_orders", "middle"], JoinType::LeftSemi, &["id"]).await.is_err());
    assert!(session.join_many_tables_using(&["left_orders", "middle"], JoinType::Inner, &["missing"]).await.is_err());
    session.register_sql_view(
        "overlap", "SELECT CAST(2 AS BIGINT) AS id, 'duplicate' AS orders"
    ).await?;
    assert!(session.join_many_tables_using(&["left_orders", "overlap"], JoinType::Inner, &["id"]).await.is_err());
    Ok(())
}
