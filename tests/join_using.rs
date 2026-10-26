use datafusion::{arrow::array::Int64Array, logical_expr::JoinType, prelude::col};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn session_with_tables() -> Result<RustSparkSession, Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "order",
            "SELECT CAST(1 AS BIGINT) AS id, 'left1' AS left_payload
             UNION ALL SELECT 2, 'left2'",
        )
        .await?;
    session
        .register_sql_view(
            "lookup",
            "SELECT CAST(2 AS BIGINT) AS id, 'right2' AS right_payload
             UNION ALL SELECT 3, 'right3'",
        )
        .await?;
    Ok(session)
}

#[tokio::test]
async fn using_merges_join_keys_for_inner_and_outer_joins() -> TestResult {
    let session = session_with_tables().await?;
    for (kind, expected) in [
        (JoinType::Inner, 1),
        (JoinType::Left, 2),
        (JoinType::Right, 2),
        (JoinType::Full, 3),
    ] {
        // "order" is a reserved SQL keyword, so it must be quoted by the API.
        let frame = session
            .join_tables_using("order", "lookup", kind, &["id"])
            .await?;
        let column_names: Vec<_> = frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect();
        // DataFusion can place the merged key after other left columns.
        assert_eq!(column_names.len(), 3);
        assert_eq!(
            column_names
                .iter()
                .filter(|name| name.as_str() == "id")
                .count(),
            1
        );
        assert_eq!(column_names[0], "id");
        assert!(column_names.contains(&"left_payload".to_owned()));
        assert!(column_names.contains(&"right_payload".to_owned()));
        assert_eq!(frame.count().await?, expected, "{kind:?}");
    }

    // A full USING join must retain the right-only key value, not NULL.
    let frame = session
        .join_tables_using("order", "lookup", JoinType::Full, &["id"])
        .await?;
    let key_column = frame
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "id")
        .expect("USING must return the shared key");
    let batches = frame
        .sort(vec![col("id").sort(true, true)])?
        .collect()
        .await?;
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column(key_column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            ids.push(column.value(index));
        }
    }
    assert_eq!(ids, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn multi_column_using_only_matches_complete_keys() -> TestResult {
    let session = session_with_tables().await?;
    session
        .register_sql_view(
            "left_pair",
            "SELECT 1 AS id, 'east' AS zone, 10 AS metric
             UNION ALL SELECT 1, 'west', 20",
        )
        .await?;
    session
        .register_sql_view(
            "right_pair",
            "SELECT 1 AS id, 'east' AS zone, 2 AS quantity
             UNION ALL SELECT 1, 'north', 4",
        )
        .await?;

    let joined = session
        .join_tables_using("left_pair", "right_pair", JoinType::Inner, &["id", "zone"])
        .await?;
    assert_eq!(joined.schema().fields().len(), 4);
    assert_eq!(joined.count().await?, 1);
    Ok(())
}

#[tokio::test]
async fn using_rejects_invalid_declarations_without_running_sql() -> TestResult {
    let session = session_with_tables().await?;
    assert!(
        session
            .join_tables_using("order", "lookup", JoinType::Inner, &[])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using("order", "lookup", JoinType::Inner, &["id", "ID"])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using("order", "lookup", JoinType::LeftSemi, &["id"])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using("order", "missing", JoinType::Inner, &["id"])
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_using("order", "lookup", JoinType::Inner, &["missing"])
            .await
            .is_err()
    );
    Ok(())
}
