use datafusion::{
    arrow::array::{Int64Array, StringArray},
    prelude::SessionConfig,
};
use rustspark::RustSparkSession;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn aggregates_csv_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;

    let batches = session
        .sql("SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city")
        .await?
        .collect()
        .await?;

    let mut rows = Vec::new();
    for batch in batches {
        let cities = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((cities.value(index).to_owned(), totals.value(index)));
        }
    }

    assert_eq!(
        rows,
        vec![("Berlin".to_owned(), 30), ("Munich".to_owned(), 5)]
    );
    Ok(())
}

#[tokio::test]
async fn unknown_table_returns_an_error() {
    let session = RustSparkSession::new();
    assert!(session.sql("SELECT * FROM missing").await.is_err());
}

#[tokio::test]
async fn filters_csv_rows() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    session.register_csv("sales", sales_csv()).await?;

    let batches = session
        .sql("SELECT COUNT(*) FROM sales WHERE amount > 5")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 2);
    Ok(())
}

#[tokio::test]
async fn sessions_have_independent_catalogs() -> Result<(), Box<dyn std::error::Error>> {
    let first = RustSparkSession::new();
    let second = RustSparkSession::new();

    first.register_csv("sales", sales_csv()).await?;

    assert!(second.sql("SELECT * FROM sales").await.is_err());
    assert!(first.sql("SELECT * FROM sales").await.is_ok());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_non_utf8_csv_paths() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    let path = PathBuf::from(OsString::from_vec(vec![0xff]));
    let error = RustSparkSession::new()
        .register_csv("data", &path)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("UTF-8"));
}

#[tokio::test]
async fn executes_queries_with_custom_session_configuration(
) -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::with_config(SessionConfig::new().with_batch_size(1));
    session.register_csv("sales", sales_csv()).await?;

    let batches = session
        .sql("SELECT COUNT(*) FROM sales")
        .await?
        .collect()
        .await?;

    assert_eq!(batches.len(), 1);
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 3);
    Ok(())
}
