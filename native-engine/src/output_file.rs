//! Atomic local result-file publication. This is not a database transaction.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use crate::{Error, Result};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

/// Writes to a private sibling file, then atomically replaces the target on
/// a successful commit. Dropping an uncommitted file removes its scratch data.
pub struct AtomicOutput {
    file: Option<File>,
    temporary: PathBuf,
    target: PathBuf,
    committed: bool,
}

impl AtomicOutput {
    pub fn create(target: impl AsRef<Path>) -> Result<Self> {
        let target = target.as_ref();
        let parent = target.parent().filter(|part| !part.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let filename = target.file_name()
            .ok_or_else(|| Error::Schema("output path requires a filename".into()))?;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|error| Error::Io(error.to_string()))?.as_nanos();
        let suffix = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{}.rustspark-{}-{timestamp}-{suffix}.tmp",
            filename.to_string_lossy(), std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&temporary)?;
        Ok(Self { file: Some(file), temporary, target: target.to_owned(), committed: false })
    }

    pub fn commit(mut self) -> Result<()> {
        self.flush()?;
        self.file.as_ref().expect("output file is open").sync_all()?;
        self.file.take();
        fs::rename(&self.temporary, &self.target)?;
        self.committed = true;
        #[cfg(unix)]
        {
            let parent = self.target.parent()
                .filter(|part| !part.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

impl Write for AtomicOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.as_mut().expect("output file is open").write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().expect("output file is open").flush()
    }
}

impl Drop for AtomicOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> PathBuf {
        let suffix = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let folder = std::env::temp_dir().join(format!(
            "rustspark-atomic-output-{}-{suffix}", std::process::id()
        ));
        fs::create_dir(&folder).unwrap();
        folder.join("result.csv")
    }

    #[test]
    fn staged_results_are_invisible_until_commit() {
        let target = target();
        let mut output = AtomicOutput::create(&target).unwrap();
        output.write_all(b"id\n1\n").unwrap();
        assert!(!target.exists());
        output.commit().unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"id\n1\n");
        fs::remove_dir_all(target.parent().unwrap()).unwrap();
    }

    #[test]
    fn dropping_failed_write_preserves_previous_result() {
        let target = target();
        fs::write(&target, "before").unwrap();
        let mut output = AtomicOutput::create(&target).unwrap();
        output.write_all(b"partial").unwrap();
        drop(output);
        assert_eq!(fs::read_to_string(&target).unwrap(), "before");
        assert_eq!(fs::read_dir(target.parent().unwrap()).unwrap().count(), 1);
        fs::remove_dir_all(target.parent().unwrap()).unwrap();
    }
}
