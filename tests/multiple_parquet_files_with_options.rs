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
    prelude::ParquetReadOptions,
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
async fn multi_file_parquet_supports_custom_file_extension()
    -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let first = dir.path().join("first.pq");
    let second = dir.path().join("second.pq");
    write_parquet(&first, 10)?;
    write_parquet(&second, 20)?;
    let files = [first, second];
    let session = RustSparkSession::new();
    assert_eq!(
        session
            .read_parquet_files_with_options(
                &files,
                ParquetReadOptions::new().file_extension(".pq"),
            )
            .await?
            .count()
            .await?,
        2
    );
    session
        .register_parquet_files_with_options(
            "sales",
            &files,
            ParquetReadOptions::new().file_extension(".pq"),
        )
        .await?;
    let batches = session
        .sql("SELECT SUM(amount) AS total FROM sales")
        .await?
        .collect()
        .await?;
    let column = batches[0].column(0);
    let totals = column.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(totals.value(0), 30);
    assert!(
        session
            .read_parquet_files_with_options::<PathBuf>(&[], ParquetReadOptions::new())
            .await
            .is_err()
    );
    assert!(
        session
            .register_parquet_files_with_options::<PathBuf>(
                "empty",
                &[],
                ParquetReadOptions::new(),
            )
            .await
            .is_err()
    );
    Ok(())
}
