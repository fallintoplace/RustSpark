use std::{path::Path, process::Command};

#[test]
fn refund_adjusted_revenue_analytics() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/refund_adjusted_revenue");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "month:utf8,order_id:int64,status:utf8,amount:int64",
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
            r#"{"month":"2026-01","gross_revenue":120,"deductions":50,"net_revenue":70}"#,
            r#"{"month":"2026-02","gross_revenue":120,"deductions":40,"net_revenue":80}"#,
        ]
    );
}
