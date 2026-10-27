use std::process::Command;

#[test]
fn supports_chained_sql_views_over_a_source_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--csv",
            "tests/fixtures/sales.csv",
            "--sql-view",
            "large=SELECT city, amount FROM data WHERE amount >= 10",
            "--sql-view",
            "totals=SELECT city, SUM(amount) AS total FROM large GROUP BY city",
            "--sql",
            "SELECT total FROM totals",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("30"));
}

#[test]
fn sql_views_work_without_input_files() {
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .args([
            "--sql-view",
            "answer=SELECT 42 AS value",
            "--sql",
            "SELECT value FROM answer",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("42"));
}

#[test]
fn rejects_invalid_view_declarations() {
    for declaration in ["missing", "123=SELECT 1", "empty=", "empty=  "] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args(["--sql-view", declaration, "--sql", "SELECT 1"])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{declaration}");
    }
}

#[test]
fn rejects_duplicate_names_and_invalid_sql() {
    for declaration in ["data=SELECT 1", "bad=SELECT * FROM missing"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--csv",
                "tests/fixtures/sales.csv",
                "--sql-view",
                declaration,
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{declaration}");
    }
}
