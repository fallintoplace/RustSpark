//! Create scratch spill files exclusively for this query process.
//! File contents may include sensitive rows, keys and intermediate results.
use std::{fs::{File, OpenOptions}, path::Path};
use crate::Result;

/// Never truncate an existing scratch file. Unix owner-only permissions are
/// set at creation to avoid a window where other users can read the file.
pub(crate) fn create_private(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn exclusive_creation_never_overwrites_an_existing_spill() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-private-spill-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = create_private(&path).unwrap();
        use std::io::Write;
        file.write_all(b"private data").unwrap();
        assert!(create_private(&path).is_err());
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"private data");
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spill_is_created_owner_read_write_only() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(
            "rustspark-private-permission-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let file = create_private(&path).unwrap();
        let mode = file.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
