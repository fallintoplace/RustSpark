use datafusion::{
    arrow::array::{Int64Array, StringArray},
    prelude::{CsvReadOptions, col, lit},
};
use rustspark::RustSparkSession;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn semicolon_csv() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sales_semicolon.csv"
    )
}

#[tokio::test]
async fn reads_csv_without_registering_table() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let frame = session
        .read_csv(sales_csv())
        .await?
        .filter(col("amount").gt(lit(5)))?
        .select_columns(&["city", "amount"])?
        .sort(vec![col("amount").sort(false, false)])?;

    assert!(session.sql("SELECT * FROM sales").await.is_err());

    let batches = frame.collect().await?;
    let mut rows = Vec::new();
    for batch in batches {
        let cities = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            rows.push((cities.value(index).to_owned(), amounts.value(index)));
        }
    }

    assert_eq!(
        rows,
        vec![("Berlin".to_owned(), 20), ("Berlin".to_owned(), 10)]
    );
    Ok(())
}

#[tokio::test]
async fn supports_custom_csv_options() -> Result<(), Box<dyn std::error::Error>> {
    let session = RustSparkSession::new();
    let matching_rows = session
        .read_csv_with_options(semicolon_csv(), CsvReadOptions::new().delimiter(b';'))
        .await?
        .filter(col("amount").gt(lit(7)))?
        .count()
        .await?;
    assert_eq!(matching_rows, 1);

    session
        .register_csv_with_options(
            "semicolon_sales",
            semicolon_csv(),
            CsvReadOptions::new().delimiter(b';'),
        )
        .await?;
    let batches = session
        .sql("SELECT SUM(amount) AS total FROM semicolon_sales")
        .await?
        .collect()
        .await?;
    let totals = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(totals.value(0), 15);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_non_utf8_dataframe_paths() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    let path = PathBuf::from(OsString::from_vec(vec![0xff]));
    let result = RustSparkSession::new().read_csv(&path).await;
    assert!(result.is_err_and(|error| error.to_string().contains("UTF-8")));
}
