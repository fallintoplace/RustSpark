use std::{fs, process::Command, time::{SystemTime, UNIX_EPOCH}};
use rustspark_native::{
    scale::{file_catalog::FileCatalog, manifest_storage},
    schema::{DataType, Field, Schema},
};

#[test]
fn streaming_cli_loads_published_catalog_in_a_new_process() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "rustspark-native-persisted-catalog-{}-{nanos}", std::process::id()
    ));
    fs::create_dir(&directory).unwrap();
    let input = directory.join("events.csv");
    let manifest = directory.join("catalog.json");
    fs::write(&input, "id\n1\n2\n3\n").unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Integer, false)
    ]).unwrap();
    let mut catalog = FileCatalog::new();
    catalog.register_csv("events", &input, schema, 4096).unwrap();
    manifest_storage::publish(&catalog, &manifest).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustspark-stream")).args([
        "--catalog-file", manifest.to_str().unwrap(),
        "--sql", "SELECT id FROM events WHERE id > 1 ORDER BY id",
    ]).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "id\n2\n3\n");
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_cli_persists_registered_source_after_successful_query() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "rustspark-native-register-{}-{nanos}", std::process::id()
    ));
    fs::create_dir(&directory).unwrap();
    let input = directory.join("events.csv");
    let manifest = directory.join("catalog.json");
    fs::write(&input, "id\n10\n20\n").unwrap();
    let created = Command::new(env!("CARGO_BIN_EXE_rustspark-stream")).args([
        "--csv", input.to_str().unwrap(), "--schema", "id:int!",
        "--table", "events", "--sql", "SELECT id FROM events",
        "--save-catalog", manifest.to_str().unwrap(),
    ]).output().unwrap();
    assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
    assert!(manifest.is_file());
    let reopened = Command::new(env!("CARGO_BIN_EXE_rustspark-stream")).args([
        "--catalog-file", manifest.to_str().unwrap(),
        "--sql", "SELECT id FROM events WHERE id >= 20",
    ]).output().unwrap();
    assert!(reopened.status.success(), "{}", String::from_utf8_lossy(&reopened.stderr));
    assert_eq!(String::from_utf8(reopened.stdout).unwrap(), "id\n20\n");
    fs::remove_dir_all(directory).unwrap();
}
