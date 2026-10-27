use std::{path::Path, process::Command};

#[test]
fn first_touch_attribution_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/first_touch_attribution");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "user_id:int64,day:int64,channel:utf8,event_name:utf8,amount:int64",
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
            r#"{"channel":"email","acquired_users":2,"attributed_revenue":40}"#,
            r#"{"channel":"search","acquired_users":2,"attributed_revenue":70}"#,
            r#"{"channel":"social","acquired_users":1,"attributed_revenue":0}"#,
        ]
    );
}
