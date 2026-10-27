use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use super::{valid_identifier, validate_table_files};

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

fn copy_backup_chunk(copies: &[(PathBuf, PathBuf)]) -> std::io::Result<()> {
    for (source, target) in copies {
        fs::copy(source, target)?;
    }
    Ok(())
}

// Copy independent immutable Parquet parts with a bounded worker count.
fn copy_backup_parts(copies: &[(PathBuf, PathBuf)]) -> Result<(), Box<dyn Error>> {
    // Large backups can copy immutable parts independently. Bound the number
    // of workers so fragmented tables do not spawn a thread per Parquet file.
    let workers = std::thread::available_parallelism()
        .map_or(1, |count| count.get())
        .min(8);
    if copies.len() < 8 || workers == 1 {
        copy_backup_chunk(copies)?;
    } else {
        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let chunk_size = copies.len().div_ceil(workers);
            let jobs = copies
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || copy_backup_chunk(chunk))
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
pub(crate) fn backup_database(root: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
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

