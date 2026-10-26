use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn fillna_supports_typed_values_for_several_columns() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_sql_view(
            "source",
            "SELECT CAST(NULL AS BIGINT) AS quantity, CAST(NULL AS VARCHAR) AS label
             UNION ALL SELECT 2, 'present'",
        )
        .await?;

    let filled = session
        .fill_nulls(
            "source",
            &[
                ("quantity", ScalarValue::from(0i64)),
                ("label", ScalarValue::from("unknown")),
            ],
        )
        .await?;
    session.register_dataframe("filled", filled)?;
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM filled WHERE quantity = 0 AND label = 'unknown'")
            .await?,
        ScalarValue::Int64(Some(1))
    );
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM filled WHERE quantity = 2 AND label = 'present'")
            .await?,
        ScalarValue::Int64(Some(1))
    );
    assert_eq!(
        session
            .sql_scalar("SELECT COUNT(*) FROM source WHERE quantity IS NULL AND label IS NULL")
            .await?,
        ScalarValue::Int64(Some(1))
    );

    assert!(
        session
            .fill_nulls("source", &[("missing", ScalarValue::from(1))])
            .await
            .is_err()
    );
    assert!(
        session
            .fill_nulls("source", &[("quantity", ScalarValue::Null)])
            .await
            .is_err()
    );
    assert!(
        session
            .fill_nulls(
                "source",
                &[
                    ("quantity", ScalarValue::from(1)),
                    ("QUANTITY", ScalarValue::from(2)),
                ],
            )
            .await
            .is_err()
    );
    Ok(())
}
