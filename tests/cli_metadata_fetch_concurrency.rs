use std::{fs::File, process::Command, sync::Arc};

use datafusion::{
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn loads_directory_with_parallel_file_metadata_reads() -> TestResult {
    let dir = tempfile::tempdir()?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        DataType::Int64,
        false,
    )]));
    for index in 0..8 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![index, index + 1]))],
        )?;
        let mut writer = ArrowWriter::try_new(
            File::create(dir.path().join(format!("part-{index}.parquet")))?,
            Arc::clone(&schema),
            None,
        )?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet")
        .arg(dir.path())
        .args([
            "--metadata-fetch-concurrency",
            "4",
            "--sql",
            "SELECT COUNT(*) AS rows FROM data",
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout)?.trim(), r#"{"rows":16}"#);
    Ok(())
}

#[test]
fn rejects_zero_metadata_concurrency() -> TestResult {
    for count in ["0", "-1", "not-a-number"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--metadata-fetch-concurrency", count, "--sql", "SELECT 1"])
            .output()?;
        assert!(!output.status.success(), "{count}");
    }
    Ok(())
}
