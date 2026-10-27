use std::{path::Path, process::Command};

#[test]
fn new_vs_returning_users_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/new_vs_returning_users");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "user_id:int64,week_id:utf8",
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
            r#"{"week_id":"w1","active_users":3,"new_users":3,"returning_users":0}"#,
            r#"{"week_id":"w2","active_users":3,"new_users":1,"returning_users":2}"#,
            r#"{"week_id":"w3","active_users":3,"new_users":1,"returning_users":2}"#,
        ]
    );
}
