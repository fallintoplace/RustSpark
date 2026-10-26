use std::collections::HashMap;

use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn binds_positional_parameters_in_lazy_views() -> TestResult {
    let session = RustSparkSession::new();
    session
        .register_csv(
            "sales",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv"),
        )
        .await?;
    session
        .register_sql_view_with_params(
            "filtered",
            "SELECT amount FROM sales WHERE amount > $1",
            vec![ScalarValue::from(5_i64)],
        )
        .await?;

    assert_eq!(session.table("filtered").await?.count().await?, 2);
    assert!(
        session
            .register_sql_view_with_params(
                "filtered",
                "SELECT 1",
                vec![ScalarValue::from(1_i64)],
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn binds_named_parameters_in_materialized_views() -> TestResult {
    let temp = tempfile::tempdir()?;
    let input = temp.path().join("sales.csv");
    std::fs::write(&input, "amount\n5\n10\n20\n")?;
    let session = RustSparkSession::new();
    session.register_csv("sales", &input).await?;

    let params = HashMap::from([("minimum".to_owned(), ScalarValue::from(10_i64))]);
    session
        .register_materialized_sql_view_with_params(
            "cached",
            "SELECT amount FROM sales WHERE amount > $minimum",
            params,
        )
        .await?;

    std::fs::remove_file(&input)?;
    assert_eq!(session.table("cached").await?.count().await?, 1);
    Ok(())
}
