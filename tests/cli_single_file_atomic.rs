use std::{fs, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

fn assert_no_staging_directories(directory: &std::path::Path) -> TestResult {
    for entry in fs::read_dir(directory)? {
        let name = entry?.file_name();
        assert!(
            !name.to_string_lossy().starts_with(".rustspark-export-"),
            "single-file export left staging data: {name:?}"
        );
    }
    Ok(())
}

#[test]
fn successful_single_file_exports_replace_previous_contents() -> TestResult {
    let dir = tempfile::tempdir()?;
    for (format, extension) in [
        ("--output-csv", "csv"),
        ("--output-json", "json"),
        ("--output-parquet", "parquet"),
    ] {
        let destination = dir.path().join(format!("result.{extension}"));
        fs::write(&destination, b"old results")?;
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--csv", sales_csv(), "--sql", "SELECT city FROM data"])
            .arg(format)
            .arg(&destination)
            .arg("--output-single-file")
            .output()?;
        assert!(
            output.status.success(),
            "{format}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let bytes = fs::read(&destination)?;
        if format == "--output-parquet" {
            assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
        } else {
            assert!(String::from_utf8(bytes)?.contains("Berlin"));
        }
        assert_no_staging_directories(dir.path())?;
    }
    Ok(())
}

#[test]
fn failed_single_file_exports_leave_no_partial_files() -> TestResult {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("invalid.csv");
    fs::write(&source, b"value\nnot_a_number\n")?;

    for (format, extension) in [
        ("--output-csv", "csv"),
        ("--output-json", "json"),
        ("--output-parquet", "parquet"),
    ] {
        for existing in [false, true] {
            let destination = dir.path().join(format!("failed-{existing}.{extension}"));
            if existing {
                fs::write(&destination, b"original results")?;
            }
            let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
                .arg("--csv")
                .arg(&source)
                .args([
                    "--csv-schema",
                    "value:int64",
                    "--sql",
                    "SELECT value FROM data",
                ])
                .arg(format)
                .arg(&destination)
                .arg("--output-single-file")
                .output()?;
            assert!(
                !output.status.success(),
                "{format} unexpectedly succeeded with invalid input"
            );
            if existing {
                assert_eq!(fs::read(&destination)?, b"original results");
            } else {
                assert!(!destination.exists(), "{format} left a partial export");
            }
            assert_no_staging_directories(dir.path())?;
        }
    }
    Ok(())
}
