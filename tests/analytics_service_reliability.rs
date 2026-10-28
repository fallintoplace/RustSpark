use std::{path::Path, process::Command};

#[test]
fn service_reliability_analytics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/analytics/service_reliability");
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark"))
        .arg("--csv")
        .arg(path.join("data.csv"))
        .args([
            "--table",
            "events",
            "--csv-schema",
            "service:utf8,request_id:int64,latency_ms:int64,status:utf8",
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
            r#"{"service":"api","requests":3,"errors":1,"slow_requests":1,"max_latency_ms":2000}"#,
            r#"{"service":"worker","requests":3,"errors":1,"slow_requests":1,"max_latency_ms":1500}"#,
        ]
    );
}
