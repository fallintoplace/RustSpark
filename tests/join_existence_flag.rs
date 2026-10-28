use datafusion::prelude::col;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn existence_flag_keeps_each_left_row_once_even_with_duplicates() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "left_rows",
            "SELECT CAST(1 AS BIGINT) AS id, 'a' AS value
             UNION ALL SELECT CAST(NULL AS BIGINT), 'null'
             UNION ALL SELECT 2, 'b'",
        )
        .await?;
    session
        .register_sql_view(
            "right_rows",
            "SELECT CAST(1 AS BIGINT) AS other_id
             UNION ALL SELECT 1
             UNION ALL SELECT CAST(NULL AS BIGINT)",
        )
        .await?;
    for (null_safe, expected) in [(false, 1), (true, 2)] {
        let flagged = session
            .join_tables_with_existence_flag(
                "left_rows",
                "right_rows",
                &[("id", "other_id")],
                "has_match",
                null_safe,
            )
            .await?;
        assert_eq!(flagged.schema().fields().len(), 3);
        assert_eq!(flagged.clone().count().await?, 3);
        assert_eq!(flagged.filter(col("has_match"))?.count().await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn existence_flag_rejects_invalid_inputs() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view("l", "SELECT 1 AS id, false AS flag")
        .await?;
    session.register_sql_view("r", "SELECT 1 AS id").await?;
    assert!(
        session
            .join_tables_with_existence_flag("l", "r", &[], "matched", false)
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_with_existence_flag("l", "r", &[("id", "id")], "FLAG", false)
            .await
            .is_err()
    );
    assert!(
        session
            .join_tables_with_existence_flag("l", "r", &[("missing", "id")], "matched", false)
            .await
            .is_err()
    );
    Ok(())
}
