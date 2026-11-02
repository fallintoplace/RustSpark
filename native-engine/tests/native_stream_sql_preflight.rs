use std::process::Command;

#[test]
fn invalid_sql_fails_before_opening_a_missing_csv_source() {
    let missing = std::env::temp_dir().join(format!(
        "rustspark-missing-{}-sql-preflight.csv", std::process::id()));
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-stream"))
        .args(["--csv", missing.to_str().unwrap(), "--schema", "id:int!",
            "--sql", "SELECT FROM"])
        .output().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.starts_with("parse:"), "{error}");
}
