use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    logical_expr::{Volatility, create_udf},
};
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn invokes_custom_scalar_function_from_sql() -> TestResult {
    let session = RustSparkSession::new();
    session.register_csv(
        "sales",
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
    ).await?;

    let identity = create_udf(
        "identity_i64",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    );
    session.register_udf(identity);

    let batches = session
        .sql("SELECT SUM(identity_i64(amount)) AS total FROM sales")
        .await?
        .collect()
        .await?;
    let totals = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(totals.value(0), 35);

    let separate_session = RustSparkSession::new();
    assert!(separate_session.sql("SELECT identity_i64(5)").await.is_err());
    Ok(())
}
