use std::{
    fs::{self, File},
    process::Command,
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

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn reads_parquet_files_from_a_relative_path_manifest() -> TestResult {
    let dir = tempfile::tempdir()?;
    let parts = dir.path().join("parts");
    fs::create_dir(&parts)?;
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    for index in 0..5 {
        let values = vec![index * 2, index * 2 + 1];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )?;
        let mut writer = ArrowWriter::try_new(
            File::create(parts.join(format!("part-{index}.parquet")))?,
            Arc::clone(&schema),
            None,
        )?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let manifest = dir.path().join("list.txt");
    fs::write(
        &manifest,
        "# Generated dataset parts\n\nparts/part-0.parquet\nparts/part-1.parquet\nparts/part-2.parquet\nparts/part-3.parquet\nparts/part-4.parquet\n",
    )?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet-file-list")
        .arg(&manifest)
        .args([
            "--table",
            "events",
            "--sql",
            "SELECT COUNT(*) AS rows, SUM(n) AS total FROM events",
            "--stdout-format",
            "json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout)?.trim(),
        r#"{"rows":10,"total":45}"#
    );

    let collision = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--parquet-file-list")
        .arg(&manifest)
        .args(["--sql", "SELECT 1", "--output-arrow"])
        .arg(parts.join("part-0.parquet"))
        .output()?;
    assert!(!collision.status.success());
    assert!(String::from_utf8_lossy(&collision.stderr).contains("overwrite an input"));
    Ok(())
}

#[test]
fn rejects_empty_or_missing_parquet_file_lists() -> TestResult {
    let dir = tempfile::tempdir()?;
    let list = dir.path().join("list.txt");
    for text in ["\n# no entries\n", "not-there.parquet\n"] {
        fs::write(&list, text)?;
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .arg("--parquet-file-list")
            .arg(&list)
            .args(["--sql", "SELECT 1"])
            .output()?;
        assert!(!output.status.success());
    }
    Ok(())
}
