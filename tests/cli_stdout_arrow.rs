use std::{io::Cursor, process::Command};

use datafusion::arrow::ipc::reader::StreamReader;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn streams_arrow_ipc_to_stdout() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42 AS answer", "--stdout-format", "arrow"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
    assert_eq!(reader.schema().field(0).name(), "answer");
    assert_eq!(reader.next().ok_or("missing batch")??.num_rows(), 1);
    assert!(reader.next().is_none());
    Ok(())
}

#[test]
fn arrow_stdout_preserves_schema_for_empty_results() -> TestResult {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql",
            "SELECT 42 AS answer WHERE FALSE",
            "--stdout-format",
            "arrow",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
    assert_eq!(reader.schema().field(0).name(), "answer");
    assert!(reader.next().is_none());
    Ok(())
}

#[test]
fn arrow_stdout_supports_result_limits() -> TestResult {
    let source = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            source,
            "--sql",
            "SELECT amount FROM data ORDER BY amount",
            "--max-rows",
            "1",
            "--stdout-format",
            "arrow",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
    let rows = reader
        .map(|batch| batch.map(|value| value.num_rows()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows.iter().sum::<usize>(), 1);
    Ok(())
}
