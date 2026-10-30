//! Atomic, single-writer publication of file catalog registration manifests.
//! This stores metadata only, not table data or a multi-statement transaction.
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use crate::{Error, Result};
use super::file_catalog::FileCatalog;

const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

struct ScratchManifest(PathBuf);
impl Drop for ScratchManifest {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Save a metadata snapshot in the target directory and atomically replace
/// the published manifest. Caller must prevent concurrent writers.
pub fn publish(catalog: &FileCatalog, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let data = catalog.export_manifest()?;
    if data.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Execution("catalog manifest exceeds byte budget".into()));
    }
    let directory = path.parent().filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name()
        .ok_or_else(|| Error::Schema("catalog manifest needs a filename".into()))?;
    let counter = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
    let time = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Io(error.to_string()))?.as_nanos();
    let temporary = directory.join(format!(
        ".{}.rustspark-{}-{time}-{counter}.tmp",
        name.to_string_lossy(), std::process::id(),
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let cleanup = ScratchManifest(temporary.clone());
    file.write_all(data.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    // On Unix, synchronizing the parent directory makes the rename durable
    // on filesystems supporting directory fsync. Other platforms may differ.
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    drop(cleanup);
    Ok(())
}

/// Read only the declared manifest budget before deserializing or registering
/// file paths. This does not validate the durability of referenced files.
pub fn open(path: impl AsRef<Path>) -> Result<FileCatalog> {
    let file = File::open(path)?;
    let mut content = String::new();
    file.take(MAX_MANIFEST_BYTES + 1).read_to_string(&mut content)?;
    if content.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Execution("catalog manifest exceeds byte budget".into()));
    }
    FileCatalog::import_manifest(&content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    fn scratch() -> PathBuf {
        let marker = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rustspark-manifest-publish-{}-{marker}", std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn manifest_survives_reopen_and_replacement() {
        let dir = scratch();
        let target = dir.join("tables.json");
        let mut registry = FileCatalog::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        registry.register_csv("events", "data/events.csv", schema.clone(), 4096).unwrap();
        publish(&registry, &target).unwrap();
        assert_eq!(open(&target).unwrap().list(), ["events"]);
        registry.register_csv("users", "data/users.csv", schema, 4096).unwrap();
        publish(&registry, &target).unwrap();
        assert_eq!(open(&target).unwrap().list(), ["events", "users"]);
        assert_eq!(fs::read_to_string(&target).unwrap(), registry.export_manifest().unwrap());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn invalid_existing_target_is_not_silently_deleted() {
        let dir = scratch();
        let target = dir.join("tables.json");
        fs::write(&target, "old").unwrap();
        assert!(open(&target).is_err());
        assert_eq!(fs::read_to_string(&target).unwrap(), "old");
        fs::remove_dir_all(dir).unwrap();
    }
}
