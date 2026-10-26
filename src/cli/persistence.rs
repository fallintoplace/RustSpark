use std::{error::Error, fs, path::Path};

use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};
use rustspark::RustSparkSession;

use super::valid_identifier;

/// Load only complete, valid table directories. Hidden staging directories and
/// symlinks are not part of the on-disk catalog.
pub(super) async fn register_database_tables(
    session: &RustSparkSession,
    root: &Path,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(root)?;
    let mut entries = fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let filename = entry.file_name();
        let Some(name) = filename.to_str() else {
            continue;
        };
        if !valid_identifier(name) {
            continue;
        }
        let mut found_parquet = false;
        for file in fs::read_dir(entry.path())? {
            let file = file?;
            if file.file_type()?.is_file()
                && file.path().extension().is_some_and(|ext| ext == "parquet")
            {
                found_parquet = true;
                break;
            }
        }
        if !found_parquet {
            return Err(format!("Database table '{name}' contains no Parquet files").into());
        }
        if session.table_exists(name)? {
            return Err(format!("Database table '{name}' conflicts with an input table").into());
        }
        register_saved_table(session, name, &entry.path()).await?;
    }
    Ok(())
}

/// Register a persistent table as a directory-backed ListingTable, not a single
/// file. DataFusion can append to directory-backed tables with SQL INSERT.
pub(super) async fn register_saved_table(
    session: &RustSparkSession,
    name: &str,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    let path = path.to_str().ok_or("Database path must be valid UTF-8")?;
    session.register_parquet(name, format!("{path}/")).await?;
    Ok(())
}

/// Write a query to a staging directory and publish it only after the write
/// succeeds. An existing table is never intentionally overwritten.
pub(super) async fn save_table(
    dataframe: DataFrame,
    root: &Path,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(root)?;
    let destination = root.join(name);
    if destination.exists() {
        return Err(format!("Database table '{name}' already exists").into());
    }
    let temporary = tempfile::Builder::new()
        .prefix(".rustspark-stage-")
        .tempdir_in(root)?;
    let staged = temporary.path().join("data");
    let staged_path = staged.to_str().ok_or("Database path must be valid UTF-8")?;
    dataframe
        .write_parquet(staged_path, DataFrameWriteOptions::new(), None)
        .await?;
    fs::rename(&staged, &destination)?;
    Ok(())
}
