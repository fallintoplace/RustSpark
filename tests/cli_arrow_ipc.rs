use std::{fs::File, process::Command};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sales_csv() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv")
}

#[tokio::test]
async fn writes_arrow_ipc_file_without_collecting_all_results() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_file = dir.path().join("sales.arrow");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            sales_csv(),
            "--batch-size",
            "1",
            "--sql",
            "SELECT city, amount FROM data ORDER BY amount",
        ])
        .arg("--output-arrow")
        .arg(&output_file)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let file = File::open(&output_file)?;
    let mut ipc = datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;
    let schema = ipc.schema();
    assert_eq!(schema.field(0).name(), "city");
    assert_eq!(schema.field(1).name(), "amount");
    let mut rows = 0;
    for batch in &mut ipc {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 3);
    Ok(())
}

#[test]
fn empty_query_still_writes_valid_ipc_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let output_file = dir.path().join("empty.arrow");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT CAST(1 AS INT) AS id WHERE FALSE"])
        .arg("--output-arrow")
        .arg(&output_file)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let file = File::open(&output_file)?;
    let mut ipc = datafusion::arrow::ipc::reader::FileReader::try_new(file, None)?;
    assert_eq!(ipc.schema().field(0).name(), "id");
    assert!(ipc.next().is_none());
    Ok(())
}

#[test]
fn disallows_conflicting_output_modes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let arrow = dir.path().join("output.arrow");
    for incompatible in ["--max-rows", "--output-csv"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1"])
            .arg("--output-arrow")
            .arg(&arrow)
            .arg(incompatible)
            .arg("1")
            .output()?;
        assert!(!output.status.success());
    }
    Ok(())
}

#[test]
fn failed_arrow_export_preserves_existing_destination() -> TestResult {
    let dir = tempfile::tempdir()?;
    let csv = dir.path().join("bad.csv");
    let destination = dir.path().join("report.arrow");
    std::fs::write(&csv, "value\nnot_a_number\n")?;
    std::fs::write(&destination, b"original report")?;
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(&csv)
        .args([
            "--csv-schema",
            "value:int64",
            "--sql",
            "SELECT value FROM data",
        ])
        .arg("--output-arrow")
        .arg(&destination)
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(std::fs::read(&destination)?, b"original report");
    Ok(())
}

#[test]
fn compressed_arrow_files_round_trip_and_reduce_size() -> TestResult {
    use datafusion::arrow::array::StringArray;

    let dir = tempfile::tempdir()?;
    let input = dir.path().join("repeated.csv");
    let payload = "abcdefgh".repeat(64);
    let mut csv = String::from("payload\n");
    for _ in 0..500 {
        csv.push_str(&payload);
        csv.push('\n');
    }
    std::fs::write(&input, csv)?;

    let mut uncompressed_size = 0;
    for codec in [None, Some("lz4"), Some("zstd")] {
        let destination = dir
            .path()
            .join(format!("{}.arrow", codec.unwrap_or("plain")));
        let mut command = Command::new(env!("CARGO_BIN_EXE_rustspark"));
        command
            .arg("--csv")
            .arg(&input)
            .args(["--sql", "SELECT payload FROM data", "--output-arrow"])
            .arg(&destination);
        if let Some(codec) = codec {
            command.args(["--arrow-compression", codec]);
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );

        let size = std::fs::metadata(&destination)?.len();
        if let Some(codec) = codec {
            assert!(
                size < uncompressed_size / 2,
                "{codec} output ({size} bytes) was not smaller than plain ({uncompressed_size} bytes)"
            );
        } else {
            uncompressed_size = size;
        }

        let mut row_count = 0;
        let reader =
            datafusion::arrow::ipc::reader::FileReader::try_new(File::open(destination)?, None)?;
        for batch in reader {
            let batch = batch?;
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(values.value(row), payload);
            }
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 500);
    }
    Ok(())
}

#[test]
fn arrow_compression_requires_an_ipc_file_and_known_codec() -> TestResult {
    let missing_output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--arrow-compression", "zstd"])
        .output()?;
    assert!(!missing_output.status.success());
    assert!(String::from_utf8_lossy(&missing_output.stderr).contains("--output-arrow"));

    let dir = tempfile::tempdir()?;
    let invalid = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--output-arrow"])
        .arg(dir.path().join("invalid.arrow"))
        .args(["--arrow-compression", "gzip"])
        .output()?;
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("possible values"));
    Ok(())
}

#[test]
fn compressed_empty_arrow_file_keeps_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let destination = dir.path().join("empty.arrow");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT CAST(1 AS INT) AS id WHERE FALSE",
            "--output-arrow",
        ])
        .arg(&destination)
        .args(["--arrow-compression", "lz4"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut reader =
        datafusion::arrow::ipc::reader::FileReader::try_new(File::open(destination)?, None)?;
    assert_eq!(reader.schema().field(0).name(), "id");
    assert!(reader.next().is_none());
    Ok(())
}
