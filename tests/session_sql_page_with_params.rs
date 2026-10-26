use datafusion::{arrow::array::Int64Array, common::ScalarValue};
use rustspark::RustSparkSession;

#[tokio::test]
async fn pages_bound_queries_and_rejects_invalid_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    let query = "SELECT amount FROM sales WHERE amount >= $1 ORDER BY amount";
    let bind = || vec![ScalarValue::Int64(Some(10))];

    let batches = session
        .sql_page_with_params(query, 1, 1, bind())
        .await?
        .collect()
        .await?;
    let amount = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(amount.value(0), 20);
    assert!(
        session
            .sql_page_with_params(query, 0, 0, bind())
            .await
            .is_err()
    );
    assert!(
        session
            .sql_page_with_params(query, usize::MAX, 2, bind())
            .await
            .is_err()
    );
    Ok(())
}
