use std::{fs::File, process::Command, sync::Arc};

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};

#[test]
fn queries_parquet_with_custom_extension() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("events.data");
    let schema = Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![20, 5]))],
    )?;
    let mut writer = ArrowWriter::try_new(File::create(&file)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(&file)
        .args([
            "--parquet-extension",
            ".data",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("25"));
    Ok(())
}

#[test]
fn rejects_invalid_parquet_extension() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--parquet",
            "tests/fixtures/sales.csv",
            "--parquet-extension",
            "data",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}
