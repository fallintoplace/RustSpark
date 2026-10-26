use std::process::{Command, Output};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

#[test]
fn unions_schema_drift_and_reordered_columns() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    std::fs::write(&first, "city,amount\nBerlin,10\n")?;
    std::fs::write(&second, "amount,source\n20,imported\n")?;
    let left = format!("left={}", first.display());
    let right = format!("right={}", second.display());
    let output = run(&[
        "--csv-table",
        &left,
        "--csv-table",
        &right,
        "--union-by-name",
        "combined=left,right",
        "--sql",
        "SELECT CAST(COUNT(*) AS VARCHAR) || ',' || CAST(COUNT(city) AS VARCHAR) || ',' || CAST(COUNT(source) AS VARCHAR) || ',' || CAST(SUM(amount) AS VARCHAR) AS summary FROM combined",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("2,1,1,30"));
    Ok(())
}

#[test]
fn unions_can_be_chained_and_queried_by_sql_views() -> TestResult {
    let file = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    let left = format!("left={file}");
    let right = format!("right={file}");
    let output = run(&[
        "--csv-table",
        &left,
        "--csv-table",
        &right,
        "--union-by-name",
        "both=left,right",
        "--union-by-name",
        "triple=both,left",
        "--sql-view",
        "totals=SELECT SUM(amount) AS total FROM triple",
        "--sql",
        "SELECT total FROM totals",
    ])?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("105"));
    Ok(())
}

#[test]
fn rejects_invalid_union_declarations() -> TestResult {
    let file = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    let left = format!("left={file}");
    let right = format!("right={file}");
    for declaration in [
        "missing",
        "all=left",
        "all=left,",
        "all=,right",
        "1bad=left,right",
        "all=left,not-a-name",
        "all=left,missing",
        "left=left,right",
    ] {
        let output = run(&[
            "--csv-table",
            &left,
            "--csv-table",
            &right,
            "--union-by-name",
            declaration,
            "--sql",
            "SELECT 1",
        ])?;
        assert!(!output.status.success(), "{declaration}");
    }
    Ok(())
}
