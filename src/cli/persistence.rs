use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use datafusion::{
    dataframe::DataFrameWriteOptions, parquet::file::reader::SerializedFileReader,
    prelude::DataFrame,
};
use rustspark::RustSparkSession;

use super::valid_identifier;

// Debug builds support deterministic interruption at storage commit
// boundaries. Release builds omit the injection entirely.
#[cfg(debug_assertions)]
fn crash_after(phase: &str) {
    if std::env::var("RUSTSPARK_TEST_INSERT_EXIT").as_deref() == Ok(phase) {
        std::process::exit(97);
    }
}

#[cfg(not(debug_assertions))]
fn crash_after(_phase: &str) {}

/// A full, private copy of a persisted table. DataFusion INSERTs run against
/// this directory, so an execution failure cannot touch the visible table.
pub(super) struct StagedInsert {
    root: PathBuf,
    table: String,
    staging: PathBuf,
    data: PathBuf,
    // Remove a private stage when its asynchronous writer is cancelled.
    // Once publication starts, keep the backup for crash recovery instead.
    cleanup_on_drop: bool,
}

impl StagedInsert {
    pub(super) fn prepare(root: &Path, table: &str) -> Result<Self, Box<dyn Error>> {
        if !valid_identifier(table) {
            return Err("Persistent INSERT requires a simple table name".into());
        }
        let original = root.join(table);
        if !fs::symlink_metadata(&original)?.file_type().is_dir() {
            return Err("Persistent table must be a regular directory".into());
        }
        // Deterministic name makes crash recovery independent of ephemeral ids.
        // The caller holds the exclusive database lock for this session.
        let staging = root.join(format!(".rustspark-insert-{table}"));
        fs::create_dir(&staging)?;
        let prepared = (|| -> Result<(), Box<dyn Error>> {
            let new = staging.join("new");
            fs::create_dir(&new)?;
            validate_table_files(&original, table)?;
            for entry in fs::read_dir(&original)? {
                let entry = entry?;
                // Copy, never hard-link: a future writer using a fixed file
                // name must not overwrite a live Parquet inode.
                fs::copy(entry.path(), new.join(entry.file_name()))?;
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
        Ok(Self {
            root: root.to_path_buf(),
            table: table.to_owned(),
            data: staging.join("new"),
            staging,
            cleanup_on_drop: true,
        })
    }

    pub(super) fn data_path(&self) -> &Path {
        // There is one staged data directory per persistent INSERT.
        // The directory exists until commit or discard.
        &self.data
    }

    pub(super) fn discard(self) -> Result<(), Box<dyn Error>> {
        fs::remove_dir_all(&self.staging)?;
        Ok(())
    }

    pub(super) fn commit(mut self) -> Result<(), Box<dyn Error>> {
        let new = self.staging.join("new");
        validate_table_files(&new, &self.table)?;
        // Non-crashing failure injection checks that an INSERT never reports
        // success before a durable publication. Release builds omit the hook.
        #[cfg(debug_assertions)]
        if std::env::var("RUSTSPARK_TEST_INSERT_COMMIT_ERROR").as_deref() == Ok("before_swap") {
            return Err("Injected pre-publication INSERT failure".into());
        }
        crash_after("before_swap");
        // The lock excludes other RustSpark processes. Between renames the
        // visible table may be absent, so recovery restores "old" on restart.
        let original = self.root.join(&self.table);
        let old = self.staging.join("old");
        fs::rename(&original, &old)?;
        // The old data is now only present inside the stage. Never delete
        // that backup on an error while publishing the replacement.
        self.cleanup_on_drop = false;
        crash_after("after_old_moved");
        if let Err(error) = fs::rename(&new, &original) {
            if let Err(restore) = fs::rename(&old, &original) {
                return Err(format!(
                    "Cannot publish INSERT for {}: {error}; recovery required: {restore}",
                    self.table
                )
                .into());
            }
            let _ = fs::remove_dir_all(&self.staging);
            return Err(error.into());
        }
        crash_after("after_publish");
        // The new table is already published. If deletion fails, startup
        // recovery cleans up the old snapshot without rolling back the write.
        let _ = fs::remove_dir_all(&self.staging);
        Ok(())
    }
}

impl Drop for StagedInsert {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            // A cancelled query may drop the future without calling discard.
            // Cleaning only an unpublished stage makes the next INSERT into
            // this table possible in the same interactive session.
            let _ = fs::remove_dir_all(&self.staging);
        }
    }
}

/// Validate every file before exposing a table. A Parquet footer check
/// catches truncated writes without scanning or materializing all rows.
fn validate_table_files(directory: &Path, name: &str) -> Result<(), Box<dyn Error>> {
    let mut found_parquet = false;
    for file in fs::read_dir(directory)? {
        let file = file?;
        let path = file.path();
        if !file.file_type()?.is_file() || !path.extension().is_some_and(|ext| ext == "parquet") {
            return Err(format!(
                "Persistent table '{name}' has an unexpected entry: {}",
                path.display()
            )
            .into());
        }
        SerializedFileReader::new(fs::File::open(&path)?).map_err(|error| {
            format!(
                "Cannot read persistent Parquet file '{}': {error}",
                path.display()
            )
        })?;
        found_parquet = true;
    }
    if !found_parquet {
        return Err(format!("Database table '{name}' contains no Parquet files").into());
    }
    Ok(())
}

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
    if let Ok(metadata) = fs::symlink_metadata(&lock_path)
        && !metadata.file_type().is_file()
    {
        return Err("Database lock path must be a regular file".into());
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

/// Restore the old snapshot if a crash left no visible table.
fn recover_interrupted_insert(
    root: &Path,
    table: &str,
    staging: &Path,
) -> Result<(), Box<dyn Error>> {
    let live = root.join(table);
    let old = staging.join("old");
    match fs::symlink_metadata(&live) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            // The new data has been published, or the old table is still
            // live. Never discard a valid backup before verifying the live
            // Parquet parts, or a failed publication could destroy recovery.
            if let Err(corruption) = validate_table_files(&live, table) {
                if !old.is_dir() {
                    return Err(format!(
                        "Invalid live table '{table}' and no recoverable backup: {corruption}"
                    )
                    .into());
                }
                validate_table_files(&old, table).map_err(|backup_error| {
                    format!(
                        "Both live and backup snapshots of '{table}' are invalid: live: {corruption}; backup: {backup_error}"
                    )
                })?;

                // Retain the corrupt snapshot outside the reserved stage.
                // Startup skips this hidden directory. Keeping it also
                // protects evidence if restoration fails halfway through.
                let quarantine = tempfile::Builder::new()
                    .prefix(".rustspark-corrupt-")
                    .tempdir_in(root)?
                    .keep();
                let corrupt_path = quarantine.join(table);
                fs::rename(&live, &corrupt_path)?;
                if let Err(restore_error) = fs::rename(&old, &live) {
                    return Err(format!(
                        "Cannot restore valid INSERT backup for '{table}': {restore_error}; corrupt data retained at {}",
                        corrupt_path.display()
                    )
                    .into());
                }
                eprintln!(
                    "Recovered previous snapshot for '{table}'; corrupt data retained at {}",
                    corrupt_path.display()
                );
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Interrupted between moving out the old snapshot and
            // publishing the new one. Validate before restoring it.
            if !fs::symlink_metadata(&old)?.file_type().is_dir() {
                return Err(format!("Missing INSERT backup for table '{table}'").into());
            }
            validate_table_files(&old, table)?;
            fs::rename(&old, &live)?;
        }
        Ok(_) => {
            return Err(format!("Invalid persistent table path: {}", live.display()).into());
        }
        Err(error) => return Err(error.into()),
    }
    fs::remove_dir_all(staging)?;
    Ok(())
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
        if let Some(table) = name.strip_prefix(".rustspark-insert-") {
            if !valid_identifier(table) || !entry.file_type()?.is_dir() {
                return Err(format!("Invalid interrupted INSERT directory: {name}").into());
            }
            recover_interrupted_insert(root, table, &entry.path())?;
            continue;
        }
        let reserved =
            name.starts_with(".rustspark-stage-") || name.starts_with(".rustspark-drop-");
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
        validate_table_files(&entry.path(), name)?;
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
    match fs::symlink_metadata(&destination) {
        Ok(_) => return Err(format!("Database table '{name}' already exists").into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
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
