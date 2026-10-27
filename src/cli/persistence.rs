use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use datafusion::{
    config::TableParquetOptions, dataframe::DataFrameWriteOptions,
    parquet::file::reader::SerializedFileReader, prelude::DataFrame,
};
use futures::{StreamExt, stream};
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

fn require_regular_directory(path: &Path, message: &'static str) -> Result<(), Box<dyn Error>> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(message.into());
    }
    Ok(())
}

fn prepare_staged_insert_files(
    original: &Path,
    staging: &Path,
    table: &str,
) -> Result<(), Box<dyn Error>> {
    let new = staging.join("new");
    fs::create_dir(&new)?;
    validate_table_files(original, table)?;
    for entry in fs::read_dir(original)? {
        let entry = entry?;
        let source = entry.path();
        let target = new.join(entry.file_name());
        // ListingTable INSERT is append-only and writes fresh Parquet
        // parts. Existing parts are immutable, so they can be linked
        // into the private replacement snapshot without copying all
        // their bytes. On filesystems without hard links, copy them.
        if let Err(link_error) = fs::hard_link(&source, &target) {
            fs::copy(&source, &target).map_err(|copy_error| {
                format!(
                    "Cannot reuse Parquet part '{}': link: {link_error}; copy: {copy_error}",
                    source.display()
                )
            })?;
        }
    }
    Ok(())
}

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
        require_regular_directory(&original, "Persistent table must be a regular directory")?;
        // Deterministic name makes crash recovery independent of ephemeral ids.
        // The caller holds the exclusive database lock for this session.
        let staging = root.join(format!(".rustspark-insert-{table}"));
        fs::create_dir(&staging)?;
        if let Err(error) = prepare_staged_insert_files(&original, &staging, table) {
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

    /// Stage a full replacement without linking existing Parquet parts.
    ///
    /// The caller writes complete new parts into data_path() before commit.
    /// The existing table stays visible until the shared recovery-safe swap.
    pub(super) fn prepare_replacement(root: &Path, table: &str) -> Result<Self, Box<dyn Error>> {
        if !valid_identifier(table) {
            return Err("Persistent compaction requires a simple table name".into());
        }
        let original = root.join(table);
        require_regular_directory(&original, "Persistent table must be a regular directory")?;
        validate_table_files(&original, table)?;
        let staging = root.join(format!(".rustspark-insert-{table}"));
        fs::create_dir(&staging)?;
        let data = staging.join("new");
        if let Err(error) = fs::create_dir(&data) {
            let _ = fs::remove_dir_all(&staging);
            return Err(error.into());
        }
        Ok(Self {
            root: root.to_path_buf(),
            table: table.to_owned(),
            staging,
            data,
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

/// Read the Parquet footer without materializing the table's rows.
fn validate_parquet_part(path: &Path) -> Result<(), String> {
    let file = fs::File::open(path).map_err(|error| {
        format!(
            "Cannot read persistent Parquet file '{}': {error}",
            path.display()
        )
    })?;
    SerializedFileReader::new(file).map_err(|error| {
        format!(
            "Cannot read persistent Parquet file '{}': {error}",
            path.display()
        )
    })?;
    Ok(())
}

// Check the complete inventory before validating any footer.
fn parquet_part_inventory(directory: &Path, name: &str) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() || !path.extension().is_some_and(|ext| ext == "parquet") {
            return Err(format!(
                "Persistent table '{name}' has an unexpected entry: {}",
                path.display()
            )
            .into());
        }
        paths.push(path);
    }
    if paths.is_empty() {
        return Err(format!("Database table '{name}' contains no Parquet files").into());
    }

    Ok(paths)
}

/// Reject unexpected entries and validate Parquet footers before exposure.
/// Large part sets can validate independent file footers in parallel.
fn validate_table_files(directory: &Path, name: &str) -> Result<(), Box<dyn Error>> {
    let paths = parquet_part_inventory(directory, name)?;

    let workers = std::thread::available_parallelism()
        .map_or(1, |cores| cores.get())
        .min(8);
    let validation = if workers > 1 && paths.len() >= 16 {
        let chunk_size = paths.len().div_ceil(workers);
        std::thread::scope(|scope| {
            let jobs = paths
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        chunk
                            .iter()
                            .try_for_each(|path| validate_parquet_part(path))
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter().try_for_each(|job| {
                job.join()
                    .expect("Parquet footer validation worker panicked")
            })
        })
    } else {
        paths
            .iter()
            .try_for_each(|path| validate_parquet_part(path))
    };
    validation.map_err(|message| -> Box<dyn Error> { message.into() })
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

// Preserve the valid backup while quarantining corrupt live table files.
fn restore_corrupt_snapshot(
    root: &Path,
    live: &Path,
    old: &Path,
    table: &str,
    corruption: &dyn Error,
) -> Result<(), Box<dyn Error>> {
    if !old.is_dir() {
        return Err(format!(
            "Invalid live table '{table}' and no recoverable backup: {corruption}"
        )
        .into());
    }
    validate_table_files(old, table).map_err(|backup_error| {
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
    fs::rename(live, &corrupt_path)?;
    if let Err(restore_error) = fs::rename(old, live) {
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
    Ok(())
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
                restore_corrupt_snapshot(root, &live, &old, table, corruption.as_ref())?;
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
    let mut entries = fs::read_dir(root)?
        .map(|result| result.map(|entry| (entry.file_name(), entry)))
        .collect::<std::io::Result<Vec<_>>>()?;
    // DirEntry::file_name allocates. Cache it once instead of allocating
    // again for every comparison in a many-table catalog.
    entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

    let mut tables = Vec::new();
    for (filename, entry) in entries {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = filename.to_str() else {
            continue;
        };
        if !valid_identifier(name) {
            continue;
        }
        if session.table_exists(name)? {
            return Err(format!("Database table '{name}' conflicts with an input table").into());
        }
        tables.push((name.to_owned(), entry.path()));
    }

    // Verify all tables before exposing any of them. Each directory has
    // independent Parquet footers, so overlap the I/O for larger catalogs.
    let parallelism =
        std::thread::available_parallelism().map_or(4, |cores| cores.get().clamp(2, 16));
    if parallelism > 1 && tables.len() >= 4 {
        let chunk_size = tables.len().div_ceil(parallelism);
        let validation = std::thread::scope(|scope| {
            let jobs = tables
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || -> Result<(), String> {
                        for (name, path) in chunk {
                            validate_table_files(path, name).map_err(|error| error.to_string())?;
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            jobs.into_iter()
                .try_for_each(|job| job.join().expect("database validation worker panicked"))
        });
        validation.map_err(|error| -> Box<dyn Error> { error.into() })?;
    } else {
        for (name, path) in &tables {
            validate_table_files(path, name)?;
        }
    }

    // Registration still stays bounded after every table has passed validation.
    let mut registrations = stream::iter(
        tables
            .iter()
            .map(|(name, path)| register_saved_table(session, name, path)),
    )
    .buffer_unordered(parallelism);
    while let Some(result) = registrations.next().await {
        result?;
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

// DataFusion may emit no part files for an empty result. Keep its schema on disk.
fn write_empty_parquet_part(
    staging: &Path,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(staging)?;
    let writer = datafusion::parquet::arrow::ArrowWriter::try_new(
        fs::File::create(staging.join("part-0.parquet"))?,
        schema,
        None,
    )?;
    writer.close()?;
    Ok(())
}

/// Write a query to a staging directory and publish it only after the write
/// succeeds. An existing table is never intentionally overwritten.
pub(super) async fn save_table(
    dataframe: DataFrame,
    root: &Path,
    name: &str,
    compression: Option<&str>,
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
    let parquet_options = compression.map(|codec| {
        let mut options = TableParquetOptions::default();
        options.global.compression = Some(codec.to_owned());
        options
    });
    dataframe
        .write_parquet(staged_path, DataFrameWriteOptions::new(), parquet_options)
        .await?;
    if !staged.exists() || fs::read_dir(&staged)?.next().is_none() {
        // DataFusion emits no Parquet part for a zero-row result. Preserve
        // the schema so the empty table can still be queried after restart.
        write_empty_parquet_part(&staged, schema)?;
    }
    fs::rename(&staged, &destination)?;
    Ok(())
}

struct BackupInventory {
    tables: Vec<String>,
    copies: Vec<(PathBuf, PathBuf)>,
}

fn collect_backup_parts(source: &Path, snapshot: &Path) -> Result<BackupInventory, Box<dyn Error>> {
    let mut tables = Vec::new();
    let mut copies = Vec::new();
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let filename = entry.file_name();
        let Some(table) = filename.to_str() else {
            continue;
        };
        if !valid_identifier(table) {
            continue;
        }
        validate_table_files(&entry.path(), table)?;
        let target = snapshot.join(table);
        fs::create_dir(&target)?;
        for part in fs::read_dir(entry.path())? {
            let part = part?;
            if !part.file_type()?.is_file() {
                return Err(format!("Unexpected database part: {}", part.path().display()).into());
            }
            copies.push((part.path(), target.join(part.file_name())));
        }
        tables.push(table.to_owned());
    }

    Ok(BackupInventory { tables, copies })
}

// Copy independent immutable Parquet parts with a bounded worker count.
fn copy_backup_parts(copies: &[(PathBuf, PathBuf)]) -> Result<(), Box<dyn Error>> {
    // Large backups can copy immutable parts independently. Bound the number
    // of workers so fragmented tables do not spawn a thread per Parquet file.
    let workers = std::thread::available_parallelism()
        .map_or(1, |count| count.get())
        .min(8);
    if copies.len() < 8 || workers == 1 {
        for (source, target) in copies {
            fs::copy(source, target)?;
        }
    } else {
        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let chunk_size = copies.len().div_ceil(workers);
            let jobs = copies
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || -> std::io::Result<()> {
                        for (source, target) in chunk {
                            fs::copy(source, target)?;
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for job in jobs {
                job.join().map_err(|_| "Backup copy worker panicked")??;
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Create an independent, consistent copy of a local database while its
/// process lock is held. Copy rather than hard-link parts so later external
/// edits to one snapshot cannot corrupt the other.
pub(super) fn backup_database(root: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let source = root.canonicalize()?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()?;
    let name = destination
        .file_name()
        .ok_or("Backup destination must name a directory")?;
    let normalized = parent.join(name);
    if normalized.starts_with(&source) {
        return Err("Backup destination must be outside --database-dir".into());
    }
    if fs::symlink_metadata(&normalized).is_ok() {
        return Err(format!(
            "Backup destination already exists: {}",
            normalized.display()
        )
        .into());
    }

    let temporary = tempfile::Builder::new()
        .prefix(".rustspark-backup-stage-")
        .tempdir_in(&parent)?;
    let snapshot = temporary.path().join("snapshot");
    fs::create_dir(&snapshot)?;
    let BackupInventory { mut tables, copies } = collect_backup_parts(&source, &snapshot)?;

    copy_backup_parts(&copies)?;
    // A backup is never published without validating every copied footer.
    for table in &tables {
        validate_table_files(&snapshot.join(table), table)?;
    }
    tables.sort();
    let manifest = format!("rustspark-backup-v1\n{}\n", tables.join("\n"));
    fs::write(snapshot.join(".rustspark-backup-manifest"), manifest)?;
    // A second pre-publication check avoids overwriting a known destination.
    if fs::symlink_metadata(&normalized).is_ok() {
        return Err("Backup destination appeared during copy".into());
    }
    fs::rename(&snapshot, &normalized)?;
    Ok(())
}

/// Rewrite a local Parquet table into one part without buffering the rows.
///
/// Query execution reads the original provider while the writer targets a
/// private directory. Publication reuses the INSERT crash-recovery protocol.
/// Call with the exclusive database lock held (as CLI.run does).
pub(super) async fn compact_saved_table(
    session: &RustSparkSession,
    root: &Path,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    let dataframe = session.table(name).await?;
    let schema = dataframe.schema().inner().clone();
    let stage = StagedInsert::prepare_replacement(root, name)?;
    let part = stage.data_path().join("part-0.parquet");
    let path = part.to_str().ok_or("Database path must be valid UTF-8")?;
    dataframe
        .write_parquet(
            path,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    if !part.exists() {
        // A zero-row DataFrame may not write a Parquet part. Persist its
        // original schema so that it remains queryable after a restart.
        write_empty_parquet_part(stage.data_path(), schema)?;
    }
    stage.commit()?;
    if !session.deregister_table(name)? {
        return Err(format!("Compacted table disappeared: {name}").into());
    }
    register_saved_table(session, name, &root.join(name)).await?;
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
    require_regular_directory(&source, "Persistent table path is not a regular directory")?;
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
    require_regular_directory(
        &source,
        "Persistent source table must be a regular directory",
    )?;
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
