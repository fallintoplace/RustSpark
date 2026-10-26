use std::{error::Error, fs, path::Path};

use datafusion::{dataframe::DataFrameWriteOptions, parquet::file::reader::SerializedFileReader, prelude::DataFrame};
use rustspark::RustSparkSession;

use super::valid_identifier;

/**
 * Hold an OS-owned lock for the entire lifetime of a local database session.
 *
 * The lock file is intentionally persistent: removing it could let a second
 * process lock a different inode while another process is still writing.
 * The OS releases the lock on process exit, including abnormal termination.
 */
pub(super) fn lock_database(root: &Path) -> Result<fs::File, Box<dyn Error>> {
    fs::create_dir_all(root)?;
    let lock_path = root.join(".rustspark.lock");
    if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
        if !metadata.file_type().is_file() {
            return Err("Database lock path must be a regular file".into());
        }
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    file.try_lock().map_err(|error| {
        format!(
            "Cannot lock database directory '{}': {error}. Another RustSpark process may be using it",
            root.display()
        )
    })?;
    Ok(file)
}

/// Remove reserved staging and drop tombstone directories after an interrupted
/// session. The caller holds the exclusive database lock.
fn recover_incomplete_operations(root: &Path) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let filename = entry.file_name();
        let Some(name) = filename.to_str() else {
            continue;
        };
        let reserved = name.starts_with(".rustspark-stage-")
            || name.starts_with(".rustspark-drop-");
        // Never follow symlinks or touch user-owned tables.
        if reserved && entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

/// Load only complete, valid table directories. Hidden staging directories and
/// symlinks are not part of the on-disk catalog.
pub(super) async fn register_database_tables(
    session: &RustSparkSession,
    root: &Path,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(root)?;
    recover_incomplete_operations(root)?;
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
            let path = file.path();
            if !file.file_type()?.is_file()
                || !path.extension().is_some_and(|ext| ext == "parquet")
            {
                return Err(format!(
                    "Persistent table '{name}' has an unexpected entry: {}",
                    path.display()
                )
                .into());
            }
            // Validate Parquet footers before exposing damaged tables to SQL.
            SerializedFileReader::new(fs::File::open(&path)?).map_err(|error| {
                format!("Cannot read persistent Parquet file '{}': {error}", path.display())
            })?;
            found_parquet = true;
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
    let schema = dataframe.schema().inner().clone();
    dataframe
        .write_parquet(staged_path, DataFrameWriteOptions::new(), None)
        .await?;
    if !staged.exists() || fs::read_dir(&staged)?.next().is_none() {
        // DataFusion emits no Parquet part for a zero-row result. Preserve
        // the schema so the empty table can still be queried after restart.
        fs::create_dir_all(&staged)?;
        let writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            fs::File::create(staged.join("part-0.parquet"))?,
            schema,
            None,
        )?;
        writer.close()?;
    }
    fs::rename(&staged, &destination)?;
    Ok(())
}

/// Move a dropped table out of the visible catalog before deleting its files.
/// The hidden directory is ignored after a crash; we never delete the input
/// file registrations or arbitrary paths outside the local database root.
pub(super) fn drop_saved_table(root: &Path, name: &str) -> Result<(), Box<dyn Error>> {
    if !valid_identifier(name) {
        return Err("Persistent table name must be a SQL identifier".into());
    }
    let source = root.join(name);
    if !fs::symlink_metadata(&source)?.file_type().is_dir() {
        return Err("Persistent table path is not a regular directory".into());
    }
    let temporary = tempfile::Builder::new()
        .prefix(".rustspark-drop-")
        .tempdir_in(root)?;
    let tombstone = temporary.path().join("dropped");
    fs::rename(&source, &tombstone)?;
    fs::remove_dir_all(&tombstone)?;
    Ok(())
}

/// Move a saved table's Parquet directory and refresh the session registration.
/// A failed refresh moves the directory back and restores the original table.
pub(super) async fn rename_saved_table(
    session: &RustSparkSession,
    root: &Path,
    source_name: &str,
    destination_name: &str,
) -> Result<(), Box<dyn Error>> {
    if !valid_identifier(source_name) || !valid_identifier(destination_name) {
        return Err("Persistent table names must be SQL identifiers".into());
    }
    if source_name == destination_name || session.table_exists(destination_name)? {
        return Err(format!("Destination table already exists: {destination_name}").into());
    }
    if !session.table_exists(source_name)? {
        return Err(format!("Source table does not exist: {source_name}").into());
    }
    let source = root.join(source_name);
    let destination = root.join(destination_name);
    if !fs::symlink_metadata(&source)?.file_type().is_dir() {
        return Err("Persistent source table must be a regular directory".into());
    }
    if fs::symlink_metadata(&destination).is_ok() {
        return Err(format!("Destination table already exists: {destination_name}").into());
    }

    fs::rename(&source, &destination)?;
    let registration = async {
        if !session.deregister_table(source_name)? {
            return Err(format!("Source table disappeared: {source_name}").into());
        }
        register_saved_table(session, destination_name, &destination).await
    }
    .await;
    if let Err(error) = registration {
        let _ = session.deregister_table(destination_name);
        fs::rename(&destination, &source)?;
        if !session.table_exists(source_name)? {
            register_saved_table(session, source_name, &source).await?;
        }
        return Err(error);
    }
    Ok(())
}
