use std::process::Command;

#[test]
fn rejects_duplicate_sql_view_names() {
    for second in ["--sql-view", "--materialized-sql-view"] {
        let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
            .args([
                "--sql-view",
                "totals=SELECT 1 AS value",
                second,
                "TOTALS=SELECT 2 AS value",
                "--sql",
                "SELECT 1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Duplicate SQL view: TOTALS"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
