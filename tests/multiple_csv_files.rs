use std::{fs, path::PathBuf};

use rustspark::RustSparkSession;

#[tokio::test]
async fn reads_and_registers_multiple_csv_files() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first.csv");
    let second = directory.path().join("second.csv");
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sales.csv");
    fs::copy(fixture, &first)?;
    fs::copy(fixture, &second)?;

    let session = RustSparkSession::new();
    let files = [first, second];
    assert_eq!(session.read_csv_files(&files).await?.count().await?, 6);
    session.register_csv_files("all_sales", &files).await?;
    assert_eq!(session.table("all_sales").await?.count().await?, 6);
    assert!(session.read_csv_files::<PathBuf>(&[]).await.is_err());
    assert!(session.register_csv_files::<PathBuf>("empty", &[]).await.is_err());
    Ok(())
}
