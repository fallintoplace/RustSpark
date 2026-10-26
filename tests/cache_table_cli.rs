use std::process::Command;

#[test]
fn caches_default_table_before_running_query() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--cache-table",
            "data",
            "--sql",
            "SELECT SUM(amount) FROM data",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("35"));
}

#[test]
fn caches_more_than_one_named_input() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            "a=tests/fixtures/sales.csv",
            "--json-table",
            "b=tests/fixtures/sales.json",
            "--cache-table",
            "a",
            "--cache-table",
            "b",
            "--sql",
            "SELECT COUNT(*) FROM a JOIN b USING (city)",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains('5'));
}

#[test]
fn rejects_unknown_cache_targets() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--cache-table", "missing", "--sql", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn rejects_repeated_cache_targets_case_insensitively() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--cache-table",
            "data",
            "--cache-table",
            "DATA",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Duplicate cached table"));
}
