use std::{path::Path, process::Command};

#[test]
fn dormant_customers_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/dormant_customers");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "customer_id:int64,day:int64,event_name:utf8",
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
            r#"{"customer_id":1,"last_active_day":95,"days_inactive":5,"is_dormant":0}"#,
            r#"{"customer_id":2,"last_active_day":25,"days_inactive":75,"is_dormant":1}"#,
            r#"{"customer_id":3,"last_active_day":75,"days_inactive":25,"is_dormant":0}"#,
            r#"{"customer_id":4,"last_active_day":100,"days_inactive":0,"is_dormant":0}"#,
        ]
    );
}
