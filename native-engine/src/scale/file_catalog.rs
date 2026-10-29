//! Lightweight file-backed catalog for the DataFusion-free streaming planner.
use std::{collections::HashMap, fs::File, io::BufReader, path::{Path, PathBuf}};
use crate::{schema::Schema, Error, Result};
use super::csv_scan::{CsvRows, open_csv};

#[derive(Clone, Debug)]
pub struct CsvTable {
    path: PathBuf,
    schema: Schema,
    max_record_bytes: usize,
}

impl CsvTable {
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn path(&self) -> &Path { &self.path }
    pub fn open(&self) -> Result<CsvRows<BufReader<File>>> {
        open_csv(&self.path, self.schema.clone(), self.max_record_bytes)
    }
}

#[derive(Default, Debug)]
pub struct FileCatalog {
    tables: HashMap<String, CsvTable>,
}

fn normalized(name: &str) -> Result<String> {
    if name.is_empty() || !name.bytes().all(|c|
        c.is_ascii_alphanumeric() || c == b'_'
    ) {
        return Err(Error::Schema("expected simple ASCII table identifier".into()));
    }
    Ok(name.to_ascii_lowercase())
}

impl FileCatalog {
    pub fn new() -> Self { Self::default() }

    pub fn register_csv(
        &mut self, name: &str, path: impl AsRef<Path>,
        schema: Schema, max_record_bytes: usize
    ) -> Result<()> {
        let key = normalized(name)?;
        if schema.is_empty() || max_record_bytes == 0 {
            return Err(Error::Schema("CSV source requires a schema and byte limit".into()));
        }
        if self.tables.contains_key(&key) {
            return Err(Error::Schema(format!("file-backed table already exists: {name}")));
        }
        self.tables.insert(key, CsvTable {
            path: path.as_ref().to_path_buf(), schema, max_record_bytes
        });
        Ok(())
    }

    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let source = normalized(from)?;
        let destination = normalized(to)?;
        if !self.tables.contains_key(&source) {
            return Err(Error::Schema(format!("unknown native file table: {from}")));
        }
        if source == destination { return Ok(()); }
        if self.tables.contains_key(&destination) {
            return Err(Error::Schema(format!("file-backed table already exists: {to}")));
        }
        let table = self.tables.remove(&source).expect("source was checked");
        self.tables.insert(destination, table);
        Ok(())
    }

    pub fn unregister(&mut self, name: &str) -> Result<CsvTable> {
        let key = normalized(name)?;
        self.tables.remove(&key)
            .ok_or_else(|| Error::Schema(format!("unknown native file table: {name}")))
    }

    pub fn get(&self, name: &str) -> Result<&CsvTable> {
        let key = normalized(name)?;
        self.tables.get(&key)
            .ok_or_else(|| Error::Schema(format!("unknown native file table: {name}")))
    }

    pub fn list(&self) -> Vec<&str> {
        let mut result = self.tables.keys().map(String::as_str).collect::<Vec<_>>();
        result.sort_unstable();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static FILE_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn rename_preserves_source_metadata_and_rejects_collisions() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("Old", "unopened.csv", schema.clone(), 128).unwrap();
        catalog.register_csv("Taken", "another.csv", schema, 128).unwrap();
        assert!(catalog.rename("old", "taken").is_err());
        assert!(catalog.get("OLD").is_ok());
        catalog.rename("OLD", "New").unwrap();
        assert!(catalog.get("old").is_err());
        assert_eq!(catalog.get("new").unwrap().path(), Path::new("unopened.csv"));
        catalog.rename("new", "NEW").unwrap();
        assert!(catalog.rename("missing", "another").is_err());
    }

    #[test]
    fn unregister_does_not_touch_source_files() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("Events", "does-not-need-to-exist.csv", schema, 100).unwrap();
        let removed = catalog.unregister("EVENTS").unwrap();
        assert_eq!(removed.path(), Path::new("does-not-need-to-exist.csv"));
        assert!(catalog.get("Events").is_err());
        assert!(catalog.unregister("Events").is_err());
        assert!(catalog.list().is_empty());
    }

    #[test]
    fn table_registration_opens_file_only_when_scanned() {
        let path = std::env::temp_dir().join(format!("rustspark-catalog-{}-{}.csv",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed)));
        fs::write(&path, "n\n12\n15\n").unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("Sales_2026", &path, schema, 1024).unwrap();
        assert!(catalog.register_csv(
            "SALES_2026", &path, Schema::new(vec![]).unwrap(), 1024).is_err());
        let values = catalog.get("sales_2026").unwrap().open().unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(values, vec![vec![Value::Integer(12)], vec![Value::Integer(15)]]);
        assert_eq!(catalog.list(), ["sales_2026"]);
        fs::remove_file(path).unwrap();
    }
}
