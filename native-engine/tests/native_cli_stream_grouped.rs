use std::{fs, process::Command, sync::atomic::{AtomicU64, Ordering}};
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn default_cli_stream_flag_runs_grouped_sql_without_materializing_csv() {
    let path = std::env::temp_dir().join(format!(
        "rustspark-grouped-cli-{}-{}.csv",
        std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed),
    ));
    fs::write(&path, "region,amount\na,2\nb,7\na,3\n").unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args(["--csv", path.to_str().unwrap(), "--table", "events",
            "--stream", "--csv-schema", "region:text,amount:int64",
            "--sql", "SELECT region, SUM(amount) AS total FROM events GROUP BY region"])
        .output().unwrap();
    fs::remove_file(path).unwrap();
    assert!(result.status.success(), "{}",
        String::from_utf8_lossy(&result.stderr));
    let text = String::from_utf8(result.stdout).unwrap();
    let rows = text.lines().collect::<Vec<_>>();
    assert_eq!(rows.first(), Some(&"region,total"));
    assert_eq!(rows.len(), 3);
    assert!(rows.contains(&"a,5"));
    assert!(rows.contains(&"b,7"));
}
