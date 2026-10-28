use std::{path::Path, process::Command};

#[test]
fn cohort_returning_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/analytics/cohort_returning");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "customer_id:int64,month:utf8",
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
            r#"{"cohort":"2026-01","cohort_size":2,"returned":2}"#,
            r#"{"cohort":"2026-02","cohort_size":2,"returned":1}"#,
            r#"{"cohort":"2026-03","cohort_size":1,"returned":0}"#,
        ]
    );
}
