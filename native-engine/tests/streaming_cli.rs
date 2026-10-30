use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn fixture() -> std::path::PathBuf {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!(
        "rustspark-native-stream-{}-{unique}.csv", std::process::id(),
    ));
    fs::write(&path, "city,amount\nBerlin,2\nParis,5\nRome,8\n").unwrap();
    path
}

#[test]
fn streams_typed_csv_with_a_filter_and_global_limit() {
    let path = fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args([
            "--stream", "--csv-schema", "city:text,amount:int64",
            "--csv", path.to_str().unwrap(), "--table", "sales",
            "--sql", "SELECT city FROM sales WHERE amount > 3 LIMIT 1",
        ])
        .output().unwrap();
    fs::remove_file(&path).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "city\nParis\n");
}

#[test]
fn streaming_mode_rejects_blocking_operators() {
    let path = fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-native"))
        .args([
            "--stream", "--csv", path.to_str().unwrap(), "--table", "sales",
            "--sql", "SELECT city, COUNT(*) FROM sales GROUP BY city",
        ])
        .output().unwrap();
    fs::remove_file(&path).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported"));
}

#[test]
fn stream_cli_queries_tables_reloaded_from_atomic_manifest() {
    use rustspark_native::scale::{catalog_manifest, file_catalog::FileCatalog};
    use rustspark_native::schema::{DataType, Field, Schema};
    let dir = std::env::temp_dir().join(format!(
        "native-stream-catalog-reopen-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_nanos(),
    ));
    std::fs::create_dir(&dir).unwrap();
    let source = dir.join("events.csv");
    let manifest = dir.join("catalog.json");
    std::fs::write(&source, "id\\n1\\n2\\n").unwrap();
    let mut catalog = FileCatalog::new();
    let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
    catalog.register_csv("events", &source, schema, 1024).unwrap();
    catalog_manifest::save_atomic(&catalog, &manifest).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustspark-stream"))
        .args(["--catalog-manifest", manifest.to_str().unwrap(),
            "--sql", "SELECT id FROM events ORDER BY id"])
        .output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "id\\n1\\n2\\n");
    std::fs::remove_dir_all(dir).unwrap();
}
