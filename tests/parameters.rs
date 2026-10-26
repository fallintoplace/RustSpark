use datafusion::{arrow::array::Int64Array, common::ScalarValue};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn binds_positional_sql_parameters() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;

    let batches = session
        .sql_with_params(
            "SELECT COUNT(*) FROM sales WHERE amount > $1",
            vec![ScalarValue::from(15_i64)],
        )
        .await?
        .collect()
        .await?;
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 1);

    // A supplied value is treated as data, never as SQL syntax.
    let batches = session
        .sql_with_params(
            "SELECT COUNT(*) FROM sales WHERE city = $1",
            vec![ScalarValue::from("Berlin' OR 1=1 --")],
        )
        .await?
        .collect()
        .await?;
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 0);
    Ok(())
}
