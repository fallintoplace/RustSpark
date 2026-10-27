use std::{path::Path, process::Command};

#[test]
fn subscription_mrr_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/subscription_mrr");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "customer_id:int64,month:utf8,plan:utf8,monthly_fee:int64,is_active:int64",
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
            r#"{"month":"2026-01","paying_subscribers":2,"monthly_recurring_revenue":30}"#,
            r#"{"month":"2026-02","paying_subscribers":2,"monthly_recurring_revenue":40}"#,
            r#"{"month":"2026-03","paying_subscribers":3,"monthly_recurring_revenue":50}"#,
        ]
    );
}
