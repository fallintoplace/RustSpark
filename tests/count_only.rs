use std::process::Command;

#[test]
fn prints_only_query_result_row_count() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT city, amount FROM data WHERE amount >= 10",
            "--count-only",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "2");
}

#[test]
fn count_is_of_query_results_not_source_rows() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT COUNT(*) AS total FROM data",
            "--count-only",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "1");
}

#[test]
fn count_only_works_without_input_files() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(["--sql", "SELECT 42", "--count-only"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "1");
}

#[test]
fn count_only_rejects_other_output_modes() {
    for conflict in [
        vec!["--describe"],
        vec!["--explain"],
        vec!["--max-rows", "1"],
        vec!["--output-parquet", "ignored"],
        vec!["--output-csv", "ignored"],
        vec!["--output-json", "ignored"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--count-only"])
            .args(conflict.clone())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{conflict:?}");
    }
}
