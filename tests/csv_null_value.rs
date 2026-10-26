use std::process::Command;

#[test]
fn treats_custom_marker_as_null_and_escapes_regex_characters() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.csv");
    std::fs::write(&path, "city,amount\nBerlin,10\nNA.,20\nNAx,5\n")?;

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            path.to_str().unwrap(),
            "--csv-null-value",
            "NA.",
            "--sql",
            "SELECT COUNT(city) FROM data",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.lines().any(|line| line.trim().trim_matches('|').trim() == "2"),
        "{stdout}"
    );
    Ok(())
}

#[test]
fn marker_applies_to_named_csv_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.csv");
    std::fs::write(&path, "city,amount\nBerlin,10\nNA,20\n")?;
    let mapping = format!("sales={}", path.display());

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            &mapping,
            "--csv-null-value",
            "NA",
            "--sql",
            "SELECT COUNT(city) FROM sales",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.trim().trim_matches('|').trim() == "1")
    );
    Ok(())
}

#[test]
fn rejects_missing_or_invalid_marker() {
    let wrong_input = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 1", "--csv-null-value", "NA"])
        .output()
        .unwrap();
    assert!(!wrong_input.status.success());
    assert!(String::from_utf8_lossy(&wrong_input.stderr).contains("requires a CSV input"));

    let empty = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT 1",
            "--csv-null-value",
            "",
        ])
        .output()
        .unwrap();
    assert!(!empty.status.success());
}
