use std::{
    fs,
    process::{Command, Output},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run(args: &[&str]) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_union_distinct_aligns_columns_and_removes_duplicates() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = dir.path().join("first.csv");
    let second = dir.path().join("second.csv");
    fs::write(&first, "city,amount\nBerlin,10\nBerlin,10\n")?;
    fs::write(&second, "amount,city\n10,Berlin\n20,Munich\n")?;
    let first_arg = format!("first={}", first.display());
    let second_arg = format!("second={}", second.display());

    let output = run(&[
        "--csv-table",
        &first_arg,
        "--csv-table",
        &second_arg,
        "--union-distinct-by-name",
        "deduped=first,second",
        "--sql",
        "SELECT city, amount FROM deduped ORDER BY city",
        "--stdout-format",
        "csv",
    ])?;
    assert_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "city,amount\nBerlin,10\nMunich,20\n"
    );

    let duplicate = run(&[
        "--csv-table",
        &first_arg,
        "--csv-table",
        &second_arg,
        "--union-by-name",
        "combined=first,second",
        "--union-distinct-by-name",
        "combined=first,second",
        "--sql",
        "SELECT COUNT(*) FROM combined",
    ])?;
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("Duplicate union destination"));
    Ok(())
}
