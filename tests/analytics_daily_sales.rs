use std::{path::Path, process::Command};

#[test]
fn daily_sales_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/daily_sales");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "day:utf8,order_id:int64,status:utf8,amount:int64",
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
            r#"{"day":"2026-10-01","paid_orders":2,"paid_revenue":80}"#,
            r#"{"day":"2026-10-02","paid_orders":1,"paid_revenue":40}"#,
            r#"{"day":"2026-10-03","paid_orders":1,"paid_revenue":70}"#,
        ]
    );
}
