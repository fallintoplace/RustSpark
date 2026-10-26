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
fn compression_requires_parquet_output() -> Result<(), Box<dyn std::error::Error>> {
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
