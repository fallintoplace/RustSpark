use datafusion::arrow::array::Int64Array;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn except_tables_matches_spark_row_multiplicity() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "spark_set_left",
            concat!(
                "SELECT CAST(1 AS BIGINT) AS value ",
                "UNION ALL SELECT 1 UNION ALL SELECT 1 ",
                "UNION ALL SELECT 2 UNION ALL SELECT 2",
            ),
        )
        .await?;
    session
        .register_sql_view(
            "spark_set_right",
            "SELECT CAST(1 AS BIGINT) AS value UNION ALL SELECT 1 UNION ALL SELECT 3",
        )
        .await?;

    let mut actual = Vec::new();
    for batch in session.except_tables("spark_set_left", "spark_set_right")
        .await?
        .collect()
        .await?
    {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(values.iter().map(|value| value.unwrap()));
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![2]);
    assert!(
        session
            .except_tables(
                "",
                "spark_set_right",
            )
            .await
            .is_err()
    );
    assert!(
        session
            .except_tables(
                "spark_set_left; DROP TABLE spark_set_right",
                "spark_set_right",
            )
            .await
            .is_err()
    );
    assert!(
        session
            .except_tables(
                "spark_set_left",
                "missing_table",
            )
            .await
            .is_err()
    );
    Ok(())
}
