use std::{path::Path, process::Command};

#[test]
fn support_backlog_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/support_backlog");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "ticket_id:int64,team:utf8,opened_day:int64,closed_day:int64",
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
            r#"{"team":"billing","open_tickets":2,"overdue_open":2,"resolved":1}"#,
            r#"{"team":"infra","open_tickets":2,"overdue_open":0,"resolved":1}"#,
        ]
    );
}
