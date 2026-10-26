use std::process::Command;

#[test]
fn analyze_executes_and_prints_operator_metrics() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql",
            "SELECT SUM(amount) AS total FROM data",
            "--explain-analyze",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("physical_plan"), "{stdout}");
}

#[test]
fn analyze_cannot_be_combined_with_plan_or_describe() {
    for flag in ["--explain", "--describe"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--explain-analyze", flag])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{flag}");
    }
}

#[test]
fn analyze_cannot_be_combined_with_export_or_preview_limit() {
    for args in [
        vec!["--max-rows", "1"],
        vec!["--output-csv", "results"],
        vec!["--output-json", "results"],
        vec!["--output-parquet", "results"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql", "SELECT 1", "--explain-analyze"])
            .args(args.clone())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
    }
}
