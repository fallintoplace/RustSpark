use datafusion::arrow::array::StringArray;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn left_anti_join_tables_matches_spark_null_and_duplicate_rules() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "spark_join_left",
            "SELECT CAST(1 AS BIGINT) AS id, 'one-a' AS value
             UNION ALL SELECT 1, 'one-b'
             UNION ALL SELECT 2, 'two'
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null-left'",
        )
        .await?;
    session
        .register_sql_view(
            "spark_join_right",
            "SELECT CAST(1 AS BIGINT) AS lookup_id
             UNION ALL SELECT 1
             UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;

    let frame = session
        .left_anti_join_tables(
            "spark_join_left",
            "spark_join_right",
            &[("id", "lookup_id")],
        )
        .await?;
    assert_eq!(frame.schema().fields().len(), 2);
    let mut values = Vec::new();
    for batch in frame.collect().await? {
        let column = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values.extend(column.iter().flatten().map(str::to_owned));
    }
    values.sort();
    assert_eq!(values, vec!["null-left", "two"]);
    assert!(
        session
            .left_anti_join_tables(
                "spark_join_left",
                "spark_join_right",
                &[],
            )
            .await
            .is_err()
    );
    assert!(
        session
            .left_anti_join_tables(
                "spark_join_left",
                "spark_join_right",
                &[("", "lookup_id")],
            )
            .await
            .is_err()
    );
    assert!(
        session
            .left_anti_join_tables(
                "spark_join_left",
                "spark_join_right",
                &[("missing", "lookup_id")],
            )
            .await
            .is_err()
    );
    Ok(())
}
