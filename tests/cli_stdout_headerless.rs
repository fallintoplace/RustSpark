use std::process::Command;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

#[test]
fn streams_headerless_csv_and_tsv() -> TestResult {
    for (format, separator) in [("csv", ","), ("tsv", "\t")] {
        let output = run(&[
            "--batch-size",
            "1",
            "--sql",
            "SELECT 1 AS id, 'first' AS name UNION ALL SELECT 2, 'second' ORDER BY id",
            "--stdout-format",
            format,
            "--stdout-no-header",
        ])?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout)?,
            format!("1{separator}first\n2{separator}second\n")
        );
    }
    Ok(())
}

#[test]
fn headerless_empty_query_writes_no_data() -> TestResult {
    for format in ["csv", "tsv"] {
        let output = run(&[
            "--sql",
            "SELECT 1 AS id WHERE FALSE",
            "--stdout-format",
            format,
            "--stdout-no-header",
        ])?;
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
    }
    Ok(())
}

#[test]
fn headerless_requires_csv_or_tsv() -> TestResult {
    for format in ["json", "arrow"] {
        let output = run(&[
            "--sql",
            "SELECT 1",
            "--stdout-format",
            format,
            "--stdout-no-header",
        ])?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--stdout-no-header"),
            "invalid format should fail with a helpful error"
        );
    }
    let missing = run(&["--sql", "SELECT 1", "--stdout-no-header"])?;
    assert!(!missing.status.success());
    Ok(())
}
