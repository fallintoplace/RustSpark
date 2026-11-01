//! Crash-safe metadata snapshots for the native file-backed catalog.
//! Source CSV and Parquet files are external data, not copied by this module.
use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use serde::{Deserialize, Serialize};
use crate::{
    schema::{DataType, Field, Schema}, Error, Result,
};
use super::file_catalog::FileCatalog;

const VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_TABLES: usize = 1024;
const MAX_COLUMNS: usize = 1024;
const MAX_COLUMN_NAME_BYTES: usize = 4096;
const MAX_CSV_RECORD_BYTES: usize = 1024 * 1024;
static TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    tables: Vec<Table>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Table {
    Csv {
        name: String,
        path: PathBuf,
        fields: Vec<Column>,
        max_record_bytes: usize,
    },
    Parquet {
        name: String,
        path: PathBuf,
        fields: Vec<Column>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Column {
    name: String,
    kind: String,
    nullable: bool,
}

fn encode_fields(schema: &Schema) -> Result<Vec<Column>> {
    // The writer must obey the same column bounds as the loader.
    // Otherwise save_atomic can publish metadata that load rejects.
    if schema.is_empty() || schema.len() > MAX_COLUMNS {
        return Err(Error::Schema("catalog manifest has invalid column count".into()));
    }
    // Apply the spill reader's column-name bound before cloning names into
    // a second schema representation for the persistent manifest.
    if schema.fields().iter().any(|field| field.name.len() > MAX_COLUMN_NAME_BYTES) {
        return Err(Error::Schema("catalog manifest column name exceeds 4096 bytes".into()));
    }
    Ok(schema.fields().iter().map(|field| Column {
        name: field.name.clone(),
        kind: match field.data_type {
            DataType::Boolean => "boolean",
            DataType::Integer => "integer",
            DataType::Float => "float",
            DataType::Text => "text",
        }.into(),
        nullable: field.nullable,
    }).collect())
}

fn decode_fields(columns: Vec<Column>) -> Result<Schema> {
    if columns.is_empty() || columns.len() > MAX_COLUMNS {
        return Err(Error::Schema("catalog manifest has invalid column count".into()));
    }
    if columns.iter().any(|column| column.name.len() > MAX_COLUMN_NAME_BYTES) {
        return Err(Error::Schema("catalog manifest column name exceeds 4096 bytes".into()));
    }
    Schema::new(columns.into_iter().map(|column| {
        let data_type = match column.kind.as_str() {
            "boolean" => DataType::Boolean,
            "integer" => DataType::Integer,
            "float" => DataType::Float,
            "text" => DataType::Text,
            _ => return Err(Error::Schema("unknown catalog column type".into())),
        };
        Ok(Field::new(column.name, data_type, column.nullable))
    }).collect::<Result<Vec<_>>>()?)
}

fn canonical_source(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_file() || canonical.as_os_str().len() > 4096 {
        return Err(Error::Execution("invalid catalog source file".into()));
    }
    Ok(canonical)
}

struct LimitedManifestWriter { bytes: Vec<u8> }

impl Write for LimitedManifestWriter {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > (MAX_MANIFEST_BYTES as usize).saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("catalog manifest exceeds byte limit"));
        }
        self.bytes.try_reserve(chunk.len())
            .map_err(|_| std::io::Error::other("catalog manifest allocation failed"))?;
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn snapshot(catalog: &FileCatalog) -> Result<Vec<u8>> {
    let names = catalog.list();
    if names.len() > MAX_TABLES {
        return Err(Error::Execution("catalog table count limit exceeded".into()));
    }
    // Serialize each table immediately. Holding a Vec<Table> for 1024 wide
    // schemas can allocate far beyond the 4 MiB manifest output budget.
    let mut writer = LimitedManifestWriter { bytes: Vec::new() };
    writer.write_all(b"{\"version\":")?;
    serde_json::to_writer(&mut writer, &VERSION)
        .map_err(|err| Error::Execution(format!("encode catalog manifest: {err}")))?;
    writer.write_all(b",\"tables\":[")?;
    for (index, name) in names.into_iter().enumerate() {
        let table = if let Some(parquet) = catalog.get_parquet(name)? {
            Table::Parquet {
                name: name.to_string(),
                path: canonical_source(parquet.path())?,
                fields: encode_fields(parquet.schema())?,
            }
        } else {
            let csv = catalog.get(name)?;
            if csv.max_record_bytes() > MAX_CSV_RECORD_BYTES {
                return Err(Error::Execution("catalog CSV record limit too large".into()));
            }
            Table::Csv {
                name: name.to_string(),
                path: canonical_source(csv.path())?,
                fields: encode_fields(csv.schema())?,
                max_record_bytes: csv.max_record_bytes(),
            }
        };
        if index != 0 { writer.write_all(b",")?; }
        serde_json::to_writer(&mut writer, &table)
            .map_err(|err| Error::Execution(format!("encode catalog manifest: {err}")))?;
    }
    writer.write_all(b"]}")?;
    Ok(writer.bytes)
}

struct TempFile(PathBuf);
impl Drop for TempFile {
    fn drop(&mut self) { let _ = fs::remove_file(&self.0); }
}

// Install cleanup only after exclusive creation succeeds. A collision must
// never unlink someone else's preexisting file when the guard is dropped.
fn create_temporary(path: &Path) -> Result<(TempFile, File)> {
    let file = super::private_files::create_private(path)?;
    Ok((TempFile(path.to_path_buf()), file))
}

/// Atomically replace the catalog metadata after syncing its content.
/// This is a single-writer API; it does not provide concurrent transactions.
pub fn save_atomic(catalog: &FileCatalog, destination: &Path) -> Result<()> {
    let bytes = snapshot(catalog)?;
    let parent = destination.parent().filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|err| Error::Io(err.to_string()))?.as_nanos();
    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let temp_path = parent.join(format!(
        ".rustspark-catalog-{}-{timestamp}-{id}.tmp", std::process::id()
    ));
    let (temp, mut file) = create_temporary(&temp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp.0, destination)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Reload a persisted snapshot. Failed validation never returns a partial catalog.
pub fn load(path: &Path) -> Result<FileCatalog> {
    // A manifest must be a finite file. Opening a FIFO before checking its
    // type could block indefinitely, even with a bounded read.
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(Error::Execution("catalog manifest is not a regular file".into()));
    }
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(Error::Execution("catalog manifest exceeds byte limit".into()));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
    // Recheck size in case the file grew after the metadata probe.
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Execution("catalog manifest exceeds byte limit".into()));
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|err| Error::Parse(format!("invalid catalog manifest: {err}")))?;
    if manifest.version != VERSION || manifest.tables.len() > MAX_TABLES {
        return Err(Error::Parse("unsupported catalog manifest version or size".into()));
    }
    let mut catalog = FileCatalog::new();
    for table in manifest.tables {
        match table {
            Table::Csv { name, path, fields, max_record_bytes } => {
                if max_record_bytes == 0 || max_record_bytes > MAX_CSV_RECORD_BYTES {
                    return Err(Error::Execution("invalid catalog CSV record limit".into()));
                }
                let schema = decode_fields(fields)?;
                let source = canonical_source(&path)?;
                catalog.register_csv(&name, source, schema, max_record_bytes)?;
            }
            Table::Parquet { name, path, fields } => {
                let schema = decode_fields(fields)?;
                let source = canonical_source(&path)?;
                catalog.register_parquet(&name, source)?;
                if catalog.schema(&name)? != &schema {
                    return Err(Error::Schema("catalog Parquet schema changed since snapshot".into()));
                }
            }
        }
    }
    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn restores_file_tables_and_preserves_previous_snapshot_on_validation_failure() {
        let dir = std::env::temp_dir().join(format!(
            "native-catalog-manifest-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        ));
        fs::create_dir(&dir).unwrap();
        let source = dir.join("events.csv");
        let manifest = dir.join("catalog.json");
        fs::write(&source, "event\n7\n").unwrap();
        let schema = Schema::new(vec![Field::new("event", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("Events", &source, schema.clone(), 4096).unwrap();
        save_atomic(&catalog, &manifest).unwrap();

        let reloaded = load(&manifest).unwrap();
        assert_eq!(reloaded.list(), ["events"]);
        assert_eq!(reloaded.get("EVENTS").unwrap().max_record_bytes(), 4096);
        assert_eq!(reloaded.schema("events").unwrap(), &schema);
        let previous = fs::read(&manifest).unwrap();
        catalog.register_csv("missing", dir.join("not-found.csv"), schema, 4096).unwrap();
        assert!(save_atomic(&catalog, &manifest).is_err());
        assert_eq!(fs::read(&manifest).unwrap(), previous);
        fs::write(&manifest, b"{\"version\":999,\"tables\":[]}").unwrap();
        assert!(load(&manifest).is_err());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_corrupt_or_oversized_catalog_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "native-catalog-invalid-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("catalog.json");
        fs::write(&path, b"{\"version\":1,\"tables\":[{}]}").unwrap();
        assert!(load(&path).is_err());
        fs::write(&path, vec![b' '; MAX_MANIFEST_BYTES as usize + 1]).unwrap();
        assert!(load(&path).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod limited_manifest_writer_tests {
    use super::*;

    #[test]
    fn refuses_output_over_four_megabytes_before_allocation() {
        let mut writer = LimitedManifestWriter { bytes: Vec::new() };
        writer.write_all(&vec![b'x'; MAX_MANIFEST_BYTES as usize]).unwrap();
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.bytes.len(), MAX_MANIFEST_BYTES as usize);
    }
}

#[cfg(test)]
mod snapshot_schema_bounds_regressions {
    use super::*;
    use std::fs;

    #[test]
    fn oversized_schema_cannot_replace_a_valid_manifest() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-catalog-wide-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        ));
        fs::create_dir(&dir).unwrap();
        let csv = dir.join("source.csv");
        let metadata = dir.join("catalog.json");
        fs::write(&csv, b"id\n1\n").unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("valid", &csv,
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            1024,
        ).unwrap();
        save_atomic(&catalog, &metadata).unwrap();
        let previous = fs::read(&metadata).unwrap();

        let oversized = Schema::new((0..=MAX_COLUMNS)
            .map(|index| Field::new(format!("column_{index}"), DataType::Integer, true))
            .collect()).unwrap();
        catalog.register_csv("wide", &csv, oversized, 1024).unwrap();
        let error = save_atomic(&catalog, &metadata).unwrap_err();
        assert!(error.to_string().contains("column count"));
        assert_eq!(fs::read(&metadata).unwrap(), previous);
        assert_eq!(load(&metadata).unwrap().list(), ["valid"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn encoder_accepts_limit_but_rejects_empty_and_excessive_fields() {
        let schema = Schema::new((0..MAX_COLUMNS)
            .map(|i| Field::new(format!("f{i}"), DataType::Text, true))
            .collect()).unwrap();
        assert_eq!(encode_fields(&schema).unwrap().len(), MAX_COLUMNS);
        assert!(encode_fields(&Schema::new(vec![]).unwrap()).is_err());
    }
}

#[cfg(test)]
mod private_manifest_snapshot_tests {
    use super::*;
    use std::fs;

    #[test]
    fn failed_exclusive_create_preserves_existing_scratch_file() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-catalog-collision-{}-{}",
            std::process::id(), TEMP_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("existing.tmp");
        fs::write(&path, b"do not delete").unwrap();
        assert!(create_temporary(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"do not delete");
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn saved_catalog_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "rustspark-catalog-private-{}-{}",
            std::process::id(), TEMP_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let source = dir.join("data.csv");
        let manifest = dir.join("catalog.json");
        fs::write(&source, b"id\n1\n").unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv(
            "data", &source,
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            1024,
        ).unwrap();
        save_atomic(&catalog, &manifest).unwrap();
        assert_eq!(fs::metadata(&manifest).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(load(&manifest).unwrap().list(), ["data"]);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod manifest_regular_file_validation_tests {
    use super::*;
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static FILE_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn rejects_directories_without_attempting_a_read() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-manifest-dir-{}-{}",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        assert!(load(&dir).unwrap_err().to_string().contains("regular file"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unix_socket_nodes_without_waiting_for_data() {
        use std::os::unix::net::UnixListener;
        let dir = std::env::temp_dir().join(format!(
            "rustspark-manifest-socket-{}-{}",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("manifest.sock");
        let listener = UnixListener::bind(&path).unwrap();
        assert!(load(&path).unwrap_err().to_string().contains("regular file"));
        drop(listener);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_oversized_files_before_deserializing() {
        let path = std::env::temp_dir().join(format!(
            "rustspark-manifest-oversize-{}-{}",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::write(&path, vec![b' '; MAX_MANIFEST_BYTES as usize + 1]).unwrap();
        assert!(load(&path).unwrap_err().to_string().contains("byte limit"));
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod manifest_column_admission_regressions {
    use super::*;
    use std::fs;

    #[test]
    fn schema_field_length_is_rejected_before_metadata_copy() {
        let oversized = Schema::new(vec![Field::new(
            "x".repeat(MAX_COLUMN_NAME_BYTES + 1), DataType::Text, true,
        )]).unwrap();
        assert!(encode_fields(&oversized).err().expect("must reject long name")
            .to_string().contains("column name"));
        assert_eq!(
            encode_fields(&Schema::new(vec![Field::new(
                "x".repeat(MAX_COLUMN_NAME_BYTES), DataType::Text, true,
            )]).unwrap()).unwrap().len(),
            1
        );
        assert!(decode_fields(vec![Column {
            name: "é".repeat(MAX_COLUMN_NAME_BYTES / 2 + 1),
            kind: "text".into(),
            nullable: false,
        }]).is_err());
    }

    #[test]
    fn rejected_long_column_never_replaces_existing_snapshot() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-manifest-column-budget-{}-{}",
            std::process::id(), TEMP_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let source = directory.join("data.csv");
        let metadata = directory.join("catalog.json");
        fs::write(&source, b"key\n1\n").unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("good", &source,
            Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap(),
            1024).unwrap();
        save_atomic(&catalog, &metadata).unwrap();
        let previous = fs::read(&metadata).unwrap();
        catalog.register_csv("bad", &source, Schema::new(vec![
            Field::new("a".repeat(MAX_COLUMN_NAME_BYTES + 1), DataType::Text, true),
        ]).unwrap(), 1024).unwrap();
        assert!(save_atomic(&catalog, &metadata).is_err());
        assert_eq!(fs::read(&metadata).unwrap(), previous);
        fs::remove_dir_all(&directory).unwrap();
    }
}

#[cfg(test)]
mod streaming_manifest_serialization_regressions {
    use super::*;

    #[test]
    fn incremental_snapshot_is_byte_identical_to_serde_manifest() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-manifest-stream-{}-{}",
            std::process::id(), TEMP_ID.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&directory).unwrap();
        let source = directory.join("data.csv");
        std::fs::write(&source, b"id\n1\n").unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("data", &source, schema.clone(), 1024).unwrap();
        let expected = serde_json::to_vec(&Manifest {
            version: VERSION,
            tables: vec![Table::Csv {
                name: "data".into(),
                path: canonical_source(&source).unwrap(),
                fields: encode_fields(&schema).unwrap(),
                max_record_bytes: 1024,
            }],
        }).unwrap();
        assert_eq!(snapshot(&catalog).unwrap(), expected);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn empty_catalog_keeps_the_existing_wire_contract() {
        let expected = serde_json::to_vec(&Manifest {
            version: VERSION,
            tables: Vec::new(),
        }).unwrap();
        assert_eq!(snapshot(&FileCatalog::new()).unwrap(), expected);
    }
}
