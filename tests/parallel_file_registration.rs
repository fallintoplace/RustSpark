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
fn registers_many_mixed_file_types() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let parquet = dir.path().join("sales.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    let mut writer = ArrowWriter::try_new(File::create(&parquet)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let mut args = Vec::new();
    for index in 0..3 {
        args.extend([
            "--csv-table".to_owned(),
            format!("c{index}=tests/fixtures/sales.csv"),
            "--parquet-table".to_owned(),
            format!("p{index}={}", parquet.display()),
            "--json-table".to_owned(),
            format!("j{index}=tests/fixtures/sales.json"),
        ]);
    }
    args.extend([
        "--sql".to_owned(),
        "SELECT COUNT(*) FROM c0 CROSS JOIN p2 CROSS JOIN j1".to_owned(),
    ]);
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("18"));
    Ok(())
}

#[test]
fn rejects_duplicate_names_across_formats() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            "events=tests/fixtures/sales.csv",
            "--json-table",
            "EVENTS=tests/fixtures/sales.json",
            "--sql",
            "SELECT 1",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Duplicate table name"));
    Ok(())
}

#[test]
fn rejects_missing_json_and_parquet_paths() -> Result<(), Box<dyn std::error::Error>> {
    for (kind, path) in [
        ("--json-table", "missing=tests/fixtures/does-not-exist.json"),
        (
            "--parquet-table",
            "missing=tests/fixtures/does-not-exist.parquet",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([kind, path, "--sql", "SELECT 1"])
            .output()?;
        assert!(!output.status.success(), "{kind}");
    }
    Ok(())
}
