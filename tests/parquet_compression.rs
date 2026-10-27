use std::{fs::File, process::Command};

use datafusion::parquet::{
    basic::Compression,
    file::reader::{FileReader, SerializedFileReader},
};

#[test]
fn uses_selected_parquet_codec() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    for (codec, expected) in [
        ("uncompressed", Compression::UNCOMPRESSED),
        ("snappy", Compression::SNAPPY),
    ] {
        let file = dir.path().join(format!("{codec}.parquet"));
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--sql",
                "SELECT * FROM data",
                "--output-parquet",
            ])
            .arg(&file)
            .args(["--output-single-file", "--parquet-compression", codec])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reader = SerializedFileReader::new(File::open(&file)?)?;
        assert_eq!(
            reader.metadata().row_group(0).column(0).compression(),
            expected
        );
    }
    Ok(())
}

#[test]
fn compression_requires_parquet_output_or_save_table() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--parquet-compression", "snappy"])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--output-parquet"));
    Ok(())
}

#[test]
fn rejects_unsupported_codec() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--output-parquet"])
        .arg(dir.path().join("invalid.parquet"))
        .args(["--parquet-compression", "invalid-codec"])
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn saves_tables_with_selected_parquet_codec() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database = dir.path().join("database");
    for (codec, expected) in [
        ("snappy", Compression::SNAPPY),
        ("uncompressed", Compression::UNCOMPRESSED),
    ] {
        let table = format!("codec_{codec}");
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--sql",
                "SELECT * FROM data",
                "--database-dir",
            ])
            .arg(&database)
            .args(["--save-table", &table, "--parquet-compression", codec])
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );

        let mut parts = 0;
        for entry in std::fs::read_dir(database.join(&table))? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "parquet") {
                let reader = SerializedFileReader::new(File::open(path)?)?;
                assert!(reader.metadata().num_row_groups() > 0);
                for row_group in reader.metadata().row_groups() {
                    for column in row_group.columns() {
                        assert_eq!(column.compression(), expected);
                    }
                }
                parts += 1;
            }
        }
        assert!(parts > 0, "saved table {table} has no Parquet parts");
    }
    Ok(())
}

#[test]
fn invalid_saved_table_codec_does_not_publish_table() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database = dir.path().join("database");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1 AS value", "--database-dir"])
        .arg(&database)
        .args([
            "--save-table",
            "broken",
            "--parquet-compression",
            "unknown-codec",
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(!database.join("broken").exists());
    Ok(())
}
