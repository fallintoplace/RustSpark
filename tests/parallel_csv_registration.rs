use std::process::Command;

#[test]
fn registers_many_named_csv_tables_before_querying() {
    let mut args = Vec::new();
    for index in 0..6 {
        args.push("--csv-table".to_string());
        args.push(format!("t{index}=tests/fixtures/sales.csv"));
    }
    args.extend([
        "--sql".to_owned(),
        "SELECT COUNT(*) FROM t0 JOIN t5 USING (city)".to_owned(),
    ]);

    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
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
fn registers_many_named_tables_across_parallel_batches() {
    let mut args = Vec::new();
    for index in 0..24 {
        args.push("--csv-table".to_owned());
        args.push(format!("t{index}=tests/fixtures/sales.csv"));
    }
    args.extend([
        "--sql".to_owned(),
        "SELECT COUNT(*) AS rows FROM t0 CROSS JOIN t23".to_owned(),
        "--stdout-format".to_owned(),
        "json".to_owned(),
    ]);
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), r#"{"rows":9}"#);
}

#[test]
fn duplicate_table_names_still_fail() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            "DATA=tests/fixtures/sales.csv",
            "--csv-table",
            "data=tests/fixtures/sales.csv",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Duplicate table name"));
}

#[test]
fn missing_csv_input_still_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv-table",
            "missing=tests/fixtures/does-not-exist.csv",
            "--sql",
            "SELECT 1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
}
