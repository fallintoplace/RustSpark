use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use datafusion::parquet::file::reader::SerializedFileReader;

use super::super::valid_identifier;

// A reserved, deterministic directory permits recovery without an external WAL.
// The database lock guarantees there is at most one writer in this directory.
const PREFIX: &str = ".rustspark-insert-";

// Debug-only deterministic failpoints. Exit immediately without running Rust
// destructors to simulate an interrupted process at publication boundaries.
#[cfg(debug_assertions)]
fn failpoint(point: &str) {
    if std::env::var("RUSTSPARK_TEST_INSERT_EXIT").ok().as_deref() == Some(point) {
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
fn failpoint(_point: &str) {}

pub(crate) struct StagedInsert {
    root: PathBuf,
    name: String,
    transaction: PathBuf,
}

impl StagedInsert {
    pub(crate) fn begin(root: &Path, name: &str) -> Result<Self, Box<dyn Error>> {
        if !valid_identifier(name) {
            return Err("Persistent INSERT requires an unquoted SQL table name".into());
        }
        let source = root.join(name);
        if !fs::symlink_metadata(&source)?.file_type().is_dir() {
            return Err(format!("Cannot INSERT into non-directory table '{name}'").into());
        }
        let transaction = root.join(format!("{PREFIX}{name}"));
        fs::create_dir(&transaction)?;
        let next = transaction.join("next");
        fs::create_dir(&next)?;

        // DataFusion appends new immutable Parquet parts. Link old parts into
        // the private snapshot (falling back to copy when links are unsupported).
        // Never link symlinks or unexpected files.
        let mut parts = 0_usize;
        for entry in fs::read_dir(&source)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file()
                || !path.extension().is_some_and(|ext| ext == "parquet")
            {
                return Err(format!("Unexpected entry in saved table: {}", path.display()).into());
            }
            let to = next.join(entry.file_name());
            fs::hard_link(&path, &to).or_else(|_| fs::copy(&path, &to).map(|_| ()))?;
            parts += 1;
        }
        if parts == 0 {
            return Err("Cannot INSERT into a table without Parquet parts".into());
        }
        sync_dir(&next)?;
        sync_dir(&transaction)?;
        sync_dir(root)?;
        failpoint("after-stage");
        Ok(Self {
            root: root.to_path_buf(),
            name: name.to_owned(),
            transaction,
        })
    }

    pub(crate) fn next_dir(&self) -> PathBuf {
        self.transaction.join("next")
    }

    pub(crate) fn discard_uncommitted(&self) {
        // After moving the old directory into 'previous', recovery must decide
        // which complete snapshot is visible. Never erase a backup here.
        if !self.transaction.join("previous").exists() && self.root.join(&self.name).is_dir() {
            let _ = fs::remove_dir_all(&self.transaction);
        }
    }

    pub(crate) fn publish(&self) -> Result<(), Box<dyn Error>> {
        let next = self.transaction.join("next");
        validate_parts(&next)?;
        sync_dir(&next)?;
        sync_dir(&self.transaction)?;
        sync_dir(&self.root)?;

        let original = self.root.join(&self.name);
        let previous = self.transaction.join("previous");
        fs::rename(&original, &previous)?;
        sync_dir(&self.root)?;
        sync_dir(&self.transaction)?;
        failpoint("after-backup");

        // If interrupted here, startup restores 'previous' to its original
        // location. Once 'next' exists at that location, startup retains it.
        if let Err(error) = fs::rename(&next, &original) {
            fs::rename(&previous, &original)?;
            sync_dir(&self.root)?;
            return Err(error.into());
        }
        sync_dir(&self.root)?;
        sync_dir(&self.transaction)?;
        failpoint("after-publish");

        // The commit is already visible. Cleanup failure must not make a
        // successfully committed INSERT appear to have failed.
        let _ = fs::remove_dir_all(&previous);
        let _ = fs::remove_dir(&self.transaction);
        Ok(())
    }
}

fn validate_parts(root: &Path) -> Result<(), Box<dyn Error>> {
    let mut count = 0_usize;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() || !path.extension().is_some_and(|ext| ext == "parquet") {
            return Err(format!("Unexpected staged Parquet entry: {}", path.display()).into());
        }
        let file = fs::File::open(&path)?;
        SerializedFileReader::new(file).map_err(|error| {
            format!(
                "Cannot read staged Parquet file '{}': {error}",
                path.display()
            )
        })?;
        // Windows FlushFileBuffers requires write access to the file handle.
        #[cfg(windows)]
        let durable_file = fs::OpenOptions::new().read(true).write(true).open(&path)?;
        #[cfg(not(windows))]
        let durable_file = fs::File::open(&path)?;
        durable_file.sync_all()?;
        count += 1;
    }
    if count == 0 {
        return Err("Staged INSERT must contain at least one Parquet file".into());
    }
    Ok(())
}

/// Run before catalog registration, while the process-exclusive DB lock is held.
pub(crate) fn recover_interrupted_inserts(root: &Path) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let filename = entry.file_name();
        let Some(name) = filename.to_str().and_then(|v| v.strip_prefix(PREFIX)) else {
            continue;
        };
        if !valid_identifier(name) || !entry.file_type()?.is_dir() {
            return Err(format!(
                "Invalid interrupted INSERT directory: {}",
                entry.path().display()
            )
            .into());
        }
        let transaction = entry.path();
        let original = root.join(name);
        let previous = transaction.join("previous");
        if previous.exists() {
            if original.exists() {
                // Publication completed. Refuse to discard the previous
                // snapshot if the published files are invalid.
                validate_parts(&original)?;
            } else {
                // The first rename completed, but publication did not.
                if !fs::symlink_metadata(&previous)?.file_type().is_dir() {
                    return Err("INSERT recovery backup must be a directory".into());
                }
                fs::rename(&previous, &original)?;
                sync_dir(root)?;
            }
        } else if !original.is_dir() {
            return Err(format!("Missing original table during INSERT recovery: {name}").into());
        }
        fs::remove_dir_all(&transaction)?;
        sync_dir(root)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), Box<dyn Error>> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<(), Box<dyn Error>> {
    // Directory fsync is not portable on Windows. File flushes and same-volume
    // renames protect normal process crashes, not all power-loss scenarios.
    Ok(())
}
