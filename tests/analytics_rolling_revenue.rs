use std::{path::Path, process::Command};

#[test]
fn rolling_revenue_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/rolling_revenue");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "day:utf8,amount:int64",
            "--sql-file",
        ])
        .arg(path.join("query.sql"))
        .args(["--stdout-format", "json"])
        .output()
        .expect("run analytics example");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec![
            r#"{"day":"2026-10-01","daily_revenue":15,"trailing_three_days":15}"#,
            r#"{"day":"2026-10-02","daily_revenue":20,"trailing_three_days":35}"#,
            r#"{"day":"2026-10-03","daily_revenue":30,"trailing_three_days":65}"#,
            r#"{"day":"2026-10-04","daily_revenue":40,"trailing_three_days":90}"#,
            r#"{"day":"2026-10-05","daily_revenue":50,"trailing_three_days":120}"#,
        ]
    );
}
