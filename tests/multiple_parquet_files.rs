use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};
use rustspark::RustSparkSession;

fn write_parquet(path: &Path, value: i64) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn reads_and_registers_multiple_parquet_files() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    write_parquet(&first, 10)?;
    write_parquet(&second, 20)?;

    let session = RustSparkSession::new();
    let files = [first, second];
    assert_eq!(session.read_parquet_files(&files).await?.count().await?, 2);
    session.register_parquet_files("combined", &files).await?;
    assert_eq!(
        session
            .sql("SELECT SUM(amount) FROM combined")
            .await?
            .collect()
            .await?[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        30
    );
    assert!(session.read_parquet_files::<PathBuf>(&[]).await.is_err());
    assert!(
        session
            .register_parquet_files::<PathBuf>("empty", &[])
            .await
            .is_err()
    );
    Ok(())
}
