use std::{fs::File, path::Path, process::Command, sync::Arc};

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
    prelude::{ParquetReadOptions, col, lit},
};
use rustspark::RustSparkSession;
use tempfile::tempdir;

fn write_sales_parquet(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["Berlin", "Munich", "Berlin"])),
            Arc::new(Int64Array::from(vec![20, 5, 10])),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn reads_parquet_as_lazy_dataframe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file = dir.path().join("sales.parquet");
    write_sales_parquet(&file)?;

    let session = RustSparkSession::new();
    let count = session
        .read_parquet(&file)
        .await?
        .filter(col("amount").gt(lit(9)))?
        .count()
        .await?;
    assert_eq!(count, 2);
    assert!(session.sql("SELECT * FROM sales").await.is_err());
    Ok(())
}

#[tokio::test]
async fn queries_registered_parquet_and_dataframe_view() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file = dir.path().join("sales.parquet");
    write_sales_parquet(&file)?;

    let session = RustSparkSession::new();
    session.register_parquet("sales", &file).await?;
    let summary = session
        .sql("SELECT city, SUM(amount) AS total FROM sales GROUP BY city")
        .await?;
    session.register_dataframe("summary", summary)?;
    let batches = session
        .sql("SELECT total FROM summary WHERE city = 'Berlin'")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let totals = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(totals.value(0), 30);
    Ok(())
}

#[tokio::test]
async fn supports_custom_parquet_extension() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file = dir.path().join("sales.data");
    write_sales_parquet(&file)?;

    let session = RustSparkSession::new();
    assert!(session.read_parquet(&file).await.is_err());
    let options = ParquetReadOptions::new().file_extension(".data");
    assert_eq!(
        session
            .read_parquet_with_options(&file, options)
            .await?
            .count()
            .await?,
        3
    );

    session
        .register_parquet_with_options(
            "sales",
            &file,
            ParquetReadOptions::new().file_extension(".data"),
        )
        .await?;
    assert_eq!(session.sql("SELECT * FROM sales").await?.count().await?, 3);
    Ok(())
}

#[test]
fn cli_queries_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let file = dir.path().join("sales.parquet");
    write_sales_parquet(&file)?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(&file)
        .args(["--sql", "SELECT SUM(amount) AS total FROM data"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "35"),
        "{stdout}"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_non_utf8_parquet_paths() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    let path = PathBuf::from(OsString::from_vec(vec![0xff]));
    let session = RustSparkSession::new();
    let error = session.read_parquet(&path).await.unwrap_err();
    assert!(error.to_string().contains("UTF-8"));
}
