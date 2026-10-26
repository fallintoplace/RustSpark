use std::{fs, path::Path};

use rustspark::{RustSparkSession, SqlExportFormat};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn no_stages(root: &Path) -> TestResult {
    assert!(
        fs::read_dir(root)?
            .filter_map(Result::ok)
            .all(|item| !item.file_name().to_string_lossy().starts_with(".rustspark-api-export-")),
        "temporary exports should always be cleaned up"
    );
    Ok(())
}

#[tokio::test]
async fn exported_parquet_is_readable_and_not_rewritten() -> TestResult {
    let temp = tempfile::tempdir()?;
    let session = RustSparkSession::new();
    let destination = temp.path().join("result");
    session.export_sql(
        "SELECT 3 AS n UNION ALL SELECT 4 AS n",
        &destination,
        SqlExportFormat::Parquet,
    ).await?;
    assert_eq!(session.read_parquet(&destination).await?.count().await?, 2);
    assert!(session.export_sql(
        "SELECT 99 AS n", &destination, SqlExportFormat::Parquet
    ).await.is_err());
    assert_eq!(session.read_parquet(&destination).await?.count().await?, 2);
    no_stages(temp.path())?;
    Ok(())
}

#[tokio::test]
async fn exports_csv_and_json_to_complete_new_directories() -> TestResult {
    let temp = tempfile::tempdir()?;
    let session = RustSparkSession::new();
    for (name, format) in [
        ("csv", SqlExportFormat::Csv),
        ("json", SqlExportFormat::Json),
    ] {
        let output = temp.path().join(name);
        session.export_sql("SELECT 1 AS id", &output, format).await?;
        let parts: Vec<_> = fs::read_dir(&output)?.collect::<std::io::Result<_>>()?;
        assert!(!parts.is_empty(), "{name} export created an empty directory");
        let data = parts.iter()
            .map(|part| fs::read_to_string(part.path()))
            .collect::<Result<Vec<_>, _>>()?
            .join("");
        assert!(data.contains('1'), "{name} export is missing its row: {data}");
    }
    no_stages(temp.path())?;
    Ok(())
}

#[tokio::test]
async fn failed_exports_leave_no_part_files_or_overwrite() -> TestResult {
    let temp = tempfile::tempdir()?;
    let session = RustSparkSession::new();
    let target = temp.path().join("target");
    assert!(session.export_sql(
        "SELECT missing_column FROM no_such_table",
        &target, SqlExportFormat::Parquet,
    ).await.is_err());
    assert!(!target.exists());
    no_stages(temp.path())?;

    fs::create_dir(&target)?;
    fs::write(target.join("sentinel"), "unchanged")?;
    assert!(session.export_sql("SELECT 1", &target, SqlExportFormat::Json).await.is_err());
    assert_eq!(fs::read_to_string(target.join("sentinel"))?, "unchanged");
    no_stages(temp.path())?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn existing_symlink_cannot_be_used_as_export_destination() -> TestResult {
    let temp = tempfile::tempdir()?;
    let session = RustSparkSession::new();
    let source = temp.path().join("source");
    fs::create_dir(&source)?;
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&source, &link)?;
    assert!(session.export_sql("SELECT 1", &link, SqlExportFormat::Csv).await.is_err());
    assert!(fs::read_dir(&source)?.next().is_none());
    no_stages(temp.path())?;
    Ok(())
}
