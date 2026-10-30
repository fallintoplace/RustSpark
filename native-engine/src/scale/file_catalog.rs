//! Lightweight file-backed catalog for the DataFusion-free streaming planner.
use std::{borrow::Cow, collections::HashMap, fs::File, io::BufReader, path::{Path, PathBuf}};
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

/// Metadata-backed Parquet source. Batches are decoded only when scanned.
#[derive(Clone, Debug)]
pub struct ParquetTable {
    path: PathBuf,
    schema: Schema,
}
impl ParquetTable {
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn path(&self) -> &Path { &self.path }
    pub fn open(&self, batch_rows: usize) -> Result<crate::parquet_scan::ParquetScan> {
        crate::parquet_scan::scan(&self.path, batch_rows)
    }
}

#[derive(Default, Debug)]
pub struct FileCatalog {
    tables: HashMap<String, CsvTable>,
    parquet: HashMap<String, ParquetTable>,
}

fn normalized(name: &str) -> Result<Cow<'_, str>> {
    if !name.as_bytes().first().is_some_and(|byte|
        byte.is_ascii_alphabetic() || *byte == b'_'
    ) || !name.bytes().all(|c|
        c.is_ascii_alphanumeric() || c == b'_'
    ) {
        return Err(Error::Schema("expected simple ASCII table identifier".into()));
    }
    if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Ok(Cow::Owned(name.to_ascii_lowercase()))
    } else {
        Ok(Cow::Borrowed(name))
    }
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
        if self.tables.contains_key(key.as_ref()) || self.parquet.contains_key(key.as_ref()) {
            return Err(Error::Schema(format!("file-backed table already exists: {name}")));
        }
        self.tables.insert(key.into_owned(), CsvTable {
            path: path.as_ref().to_path_buf(), schema, max_record_bytes
        });
        Ok(())
    }

    pub fn register_parquet(&mut self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let key = normalized(name)?;
        if self.tables.contains_key(key.as_ref()) || self.parquet.contains_key(key.as_ref()) {
            return Err(Error::Schema(format!("file-backed table already exists: {name}")));
        }
        let path = path.as_ref();
        let file = File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|err| Error::Execution(format!("Parquet metadata: {err}")))?;
        let empty = arrow_array::RecordBatch::new_empty(builder.schema().clone());
        let schema = crate::arrow_bridge::from_arrow(&empty)?.schema().clone();
        self.parquet.insert(key.into_owned(), ParquetTable {
            path: path.to_path_buf(), schema,
        });
        Ok(())
    }

    pub fn schema(&self, name: &str) -> Result<&Schema> {
        let key = normalized(name)?;
        self.tables.get(key.as_ref()).map(CsvTable::schema)
            .or_else(|| self.parquet.get(key.as_ref()).map(ParquetTable::schema))
            .ok_or_else(|| Error::Schema(format!("unknown native file table: {name}")))
    }

    pub fn get_parquet(&self, name: &str) -> Result<Option<&ParquetTable>> {
        let key = normalized(name)?;
        Ok(self.parquet.get(key.as_ref()))
    }

    pub fn unregister_parquet(&mut self, name: &str) -> Result<ParquetTable> {
        let key = normalized(name)?;
        self.parquet.remove(key.as_ref())
            .ok_or_else(|| Error::Schema(format!("unknown native Parquet table: {name}")))
    }

    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let source = normalized(from)?;
        let destination = normalized(to)?;
        if !self.tables.contains_key(source.as_ref()) && !self.parquet.contains_key(source.as_ref()) {
            return Err(Error::Schema(format!("unknown native file table: {from}")));
        }
        if source == destination { return Ok(()); }
        if self.tables.contains_key(destination.as_ref()) || self.parquet.contains_key(destination.as_ref()) {
            return Err(Error::Schema(format!("file-backed table already exists: {to}")));
        }
        if let Some(table) = self.tables.remove(source.as_ref()) {
            self.tables.insert(destination.into_owned(), table);
        } else {
            let table = self.parquet.remove(source.as_ref()).expect("Parquet source was checked");
            self.parquet.insert(destination.into_owned(), table);
        }
        Ok(())
    }

    pub fn unregister(&mut self, name: &str) -> Result<CsvTable> {
        let key = normalized(name)?;
        self.tables.remove(key.as_ref())
            .ok_or_else(|| Error::Schema(format!("unknown native file table: {name}")))
    }

    pub fn get(&self, name: &str) -> Result<&CsvTable> {
        let key = normalized(name)?;
        self.tables.get(key.as_ref())
            .ok_or_else(|| Error::Schema(format!("unknown native file table: {name}")))
    }

    pub fn list(&self) -> Vec<&str> {
        let mut result = self.tables.keys().chain(self.parquet.keys())
            .map(String::as_str).collect::<Vec<_>>();
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

#[cfg(test)]
mod borrowed_file_catalog_key_perf_tests {
    use super::*;

    #[test]
    fn lowercase_keys_are_borrowed_and_casefolding_is_unchanged() {
        assert!(matches!(normalized("sales_2026").unwrap(), Cow::Borrowed(_)));
        assert!(matches!(normalized("Sales_2026").unwrap(), Cow::Owned(_)));
        assert_eq!(normalized("SALES").unwrap().as_ref(), "sales");
        assert!(normalized("a-b").is_err());
        let mut catalog = FileCatalog::new();
        let schema = Schema::new(vec![crate::schema::Field::new(
            "id", crate::schema::DataType::Integer, true,
        )]).unwrap();
        catalog.register_csv("SALES", "unused.csv", schema, 100).unwrap();
        assert!(catalog.get("sales").is_ok());
        assert!(catalog.get("Sales").is_ok());
    }
}

#[cfg(test)]
mod catalog_identifier_prefix_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn rejects_digit_prefixes_that_sql_cannot_name() {
        let mut catalog = FileCatalog::new();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        for invalid in ["1table", "2026", "", "-invalid"] {
            assert!(catalog.register_csv(invalid, "unused.csv",
                schema.clone(), 100).is_err());
        }
        catalog.register_csv("_2026", "unused.csv", schema, 100).unwrap();
        assert!(catalog.get("_2026").is_ok());
    }
}

#[cfg(test)]
mod parquet_catalog_integration_tests {
    use super::*;
    use crate::{batch::Batch, scalar::Value, schema::{DataType, Field},
        scale::{cancel::QueryControl, stream_execute::{self, StreamOptions}}};
    use parquet::arrow::ArrowWriter;

    #[test]
    fn parquet_tables_run_in_native_streaming_sql_and_rename_safely() {
        let path = std::env::temp_dir().join(format!("native-streaming-catalog-{}-{}.parquet",
            std::process::id(), std::time::SystemTime::now().duration_since(
                std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(),
            (0..6).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let arrow = crate::arrow_bridge::to_arrow(&input).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_parquet("PARQUET_SOURCE", &path).unwrap();
        assert_eq!(catalog.schema("parquet_source").unwrap(), &schema);
        assert!(catalog.register_csv("parquet_source", "unused.csv", schema.clone(), 20).is_err());
        catalog.rename("PARQUET_SOURCE", "events").unwrap();
        assert_eq!(catalog.list(), ["events"]);
        let plan = crate::sql::parse_sql("SELECT n FROM events WHERE n >= 3 LIMIT 2").unwrap();
        let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
        let rows = stream_execute::execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
        assert!(catalog.unregister_parquet("events").is_ok());
        std::fs::remove_file(path).unwrap();
    }
}


const MAX_CATALOG_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_CATALOG_TABLES: usize = 4096;

fn manifest_text<'a>(entry: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    entry.get(key).and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Schema(format!("catalog manifest requires {key}")))
}

fn manifest_type(kind: &str) -> Result<crate::schema::DataType> {
    use crate::schema::DataType;
    match kind {
        "boolean" => Ok(DataType::Boolean),
        "integer" => Ok(DataType::Integer),
        "float" => Ok(DataType::Float),
        "text" => Ok(DataType::Text),
        _ => Err(Error::Schema(format!("unsupported catalog manifest type: {kind}"))),
    }
}

fn manifest_type_name(kind: crate::schema::DataType) -> &'static str {
    use crate::schema::DataType;
    match kind {
        DataType::Boolean => "boolean",
        DataType::Integer => "integer",
        DataType::Float => "float",
        DataType::Text => "text",
    }
}

impl FileCatalog {
    /// Encode the file table registry, not table contents. Paths are references
    /// owned by the caller, and are interpreted relative to the process CWD.
    pub fn export_manifest(&self) -> Result<String> {
        let mut entries = Vec::new();
        for name in self.list() {
            if let Some(table) = self.tables.get(name) {
                let path = table.path.to_str()
                    .ok_or_else(|| Error::Schema("manifest path is not UTF-8".into()))?;
                let fields = table.schema.fields().iter().map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "type": manifest_type_name(field.data_type),
                        "nullable": field.nullable
                    })
                }).collect::<Vec<_>>();
                entries.push(serde_json::json!({
                    "name": name, "format": "csv", "path": path,
                    "max_record_bytes": table.max_record_bytes, "fields": fields
                }));
            } else if let Some(table) = self.parquet.get(name) {
                let path = table.path.to_str()
                    .ok_or_else(|| Error::Schema("manifest path is not UTF-8".into()))?;
                entries.push(serde_json::json!({
                    "name": name, "format": "parquet", "path": path
                }));
            }
        }
        if entries.len() > MAX_CATALOG_TABLES {
            return Err(Error::Execution("catalog table count exceeds manifest budget".into()));
        }
        let text = serde_json::json!({"version": 1, "tables": entries}).to_string();
        if text.len() > MAX_CATALOG_MANIFEST_BYTES {
            return Err(Error::Execution("catalog manifest exceeds byte budget".into()));
        }
        Ok(text)
    }

    /// Restore a registry from validated metadata. Existing source files are
    /// not copied. Parquet files are validated when registered.
    pub fn import_manifest(text: &str) -> Result<Self> {
        if text.len() > MAX_CATALOG_MANIFEST_BYTES {
            return Err(Error::Execution("catalog manifest exceeds byte budget".into()));
        }
        let root: serde_json::Value = serde_json::from_str(text)
            .map_err(|error| Error::Parse(format!("invalid catalog manifest: {error}")))?;
        if root.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(Error::Schema("unsupported catalog manifest version".into()));
        }
        let tables = root.get("tables").and_then(serde_json::Value::as_array)
            .ok_or_else(|| Error::Schema("catalog manifest needs tables array".into()))?;
        if tables.len() > MAX_CATALOG_TABLES {
            return Err(Error::Execution("catalog table count exceeds manifest budget".into()));
        }
        let mut catalog = Self::new();
        for table in tables {
            let name = manifest_text(table, "name")?;
            let format = manifest_text(table, "format")?;
            let path = manifest_text(table, "path")?;
            if path.len() > 4096 {
                return Err(Error::Schema("catalog manifest path too long".into()));
            }
            match format {
                "csv" => {
                    let max = table.get("max_record_bytes")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok())
                        .filter(|&value| value > 0)
                        .ok_or_else(|| Error::Schema("invalid catalog CSV byte limit".into()))?;
                    let fields = table.get("fields").and_then(serde_json::Value::as_array)
                        .ok_or_else(|| Error::Schema("catalog CSV requires fields array".into()))?;
                    if fields.is_empty() || fields.len() > 4096 {
                        return Err(Error::Schema("catalog CSV field count out of range".into()));
                    }
                    let fields = fields.iter().map(|field| {
                        let name = manifest_text(field, "name")?;
                        let kind = manifest_type(manifest_text(field, "type")?)?;
                        let nullable = field.get("nullable").and_then(serde_json::Value::as_bool)
                            .ok_or_else(|| Error::Schema("catalog column nullability missing".into()))?;
                        Ok(crate::schema::Field::new(name, kind, nullable))
                    }).collect::<Result<Vec<_>>>()?;
                    catalog.register_csv(name, path, Schema::new(fields)?, max)?;
                }
                "parquet" => catalog.register_parquet(name, path)?,
                _ => return Err(Error::Schema("unknown catalog manifest table format".into())),
            }
        }
        Ok(catalog)
    }
}

#[cfg(test)]
mod file_catalog_manifest_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn snapshots_are_stable_and_recover_csv_table_schema() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, true),
        ]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("Events", "somewhere/events.csv", schema.clone(), 8192).unwrap();
        catalog.register_csv("Users", "somewhere/users.csv", schema, 4096).unwrap();
        let manifest = catalog.export_manifest().unwrap();
        let restored = FileCatalog::import_manifest(&manifest).unwrap();
        assert_eq!(restored.export_manifest().unwrap(), manifest);
        assert_eq!(restored.list(), ["events", "users"]);
        assert_eq!(restored.get("EVENTS").unwrap().max_record_bytes, 8192);
    }

    #[test]
    fn invalid_manifest_does_not_publish_partial_registry() {
        for text in [
            "{}",
            r#"{"version":2,"tables":[]}"#,
            r#"{"version":1,"tables":"wrong"}"#,
            r#"{"version":1,"tables":[{"name":"one","format":"csv","path":"a.csv","max_record_bytes":1,"fields":[{"name":"x","type":"nonsense","nullable":true}]}]}"#,
            r#"{"version":1,"tables":[{"name":"one","format":"csv","path":"a.csv","max_record_bytes":0,"fields":[]}]}"#,
        ] {
            assert!(FileCatalog::import_manifest(text).is_err(), "{text}");
        }
    }
}

static CATALOG_SNAPSHOT_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

struct IncompleteManifest(std::path::PathBuf);

impl Drop for IncompleteManifest {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
}

impl FileCatalog {
    /// Atomically replace one local catalog snapshot. Intended for a single
    /// writer; callers must coordinate concurrent writers separately.
    pub fn save_manifest_atomic(&self, path: impl AsRef<Path>) -> Result<()> {
        use std::io::Write;
        let path = path.as_ref();
        let contents = self.export_manifest()?;
        let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = path.file_name()
            .ok_or_else(|| Error::Schema("catalog snapshot path needs a filename".into()))?;
        let id = CATALOG_SNAPSHOT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut filename = name.to_os_string();
        filename.push(format!(".tmp-{}-{id}", std::process::id()));
        let staged = IncompleteManifest(parent.join(filename));
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true)
            .open(&staged.0)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&staged.0, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }

    /// Read only bounded manifest metadata. Referenced source files are not
    /// implicitly copied into the catalog directory.
    pub fn load_manifest_file(path: impl AsRef<Path>) -> Result<Self> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        if file.metadata()?.len() > MAX_CATALOG_MANIFEST_BYTES as u64 {
            return Err(Error::Execution("catalog manifest exceeds byte budget".into()));
        }
        let mut contents = String::new();
        file.take(MAX_CATALOG_MANIFEST_BYTES as u64 + 1).read_to_string(&mut contents)?;
        Self::import_manifest(&contents)
    }
}

#[cfg(test)]
mod native_catalog_atomic_snapshot_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn snapshot_survives_reopening_and_replaces_previous_generation() {
        let directory = std::env::temp_dir().join(format!(
            "native-catalog-snapshot-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("catalog.json");
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("first", "first.csv", schema.clone(), 128).unwrap();
        catalog.save_manifest_atomic(&path).unwrap();
        assert_eq!(FileCatalog::load_manifest_file(&path).unwrap().list(), ["first"]);
        catalog.register_csv("second", "second.csv", schema, 128).unwrap();
        catalog.save_manifest_atomic(&path).unwrap();
        assert_eq!(FileCatalog::load_manifest_file(&path).unwrap().list(), ["first", "second"]);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_saved_snapshot_fails_closed() {
        let path = std::env::temp_dir().join(format!(
            "native-invalid-catalog-{}-{}.json", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos()
        ));
        std::fs::write(&path, "{").unwrap();
        assert!(FileCatalog::load_manifest_file(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
