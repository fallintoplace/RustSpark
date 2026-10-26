use std::{fs, path::PathBuf};

use rustspark::RustSparkSession;

#[tokio::test]
async fn reads_and_registers_multiple_json_files() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first.json");
    let second = directory.path().join("second.json");
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.json");
    fs::copy(fixture, &first)?;
    fs::copy(fixture, &second)?;

    let session = RustSparkSession::new();
    let files = [first, second];
    assert_eq!(session.read_json_files(&files).await?.count().await?, 6);
    session.register_json_files("all_sales", &files).await?;
    assert_eq!(session.table("all_sales").await?.count().await?, 6);
    assert!(session.read_json_files::<PathBuf>(&[]).await.is_err());
    assert!(session.register_json_files::<PathBuf>("empty", &[]).await.is_err());
    Ok(())
}
