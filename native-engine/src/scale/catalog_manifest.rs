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
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        if let Some(parquet) = catalog.get_parquet(name)? {
            tables.push(Table::Parquet {
                name: name.to_string(),
                path: canonical_source(parquet.path())?,
                fields: encode_fields(parquet.schema())?,
            });
        } else {
            let csv = catalog.get(name)?;
            if csv.max_record_bytes() > MAX_CSV_RECORD_BYTES {
                return Err(Error::Execution("catalog CSV record limit too large".into()));
            }
            tables.push(Table::Csv {
                name: name.to_string(),
                path: canonical_source(csv.path())?,
                fields: encode_fields(csv.schema())?,
                max_record_bytes: csv.max_record_bytes(),
            });
        }
    }
    let mut writer = LimitedManifestWriter { bytes: Vec::new() };
    serde_json::to_writer(&mut writer, &Manifest { version: VERSION, tables })
        .map_err(|err| Error::Execution(format!("encode catalog manifest: {err}")))?;
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
    let mut bytes = Vec::new();
    File::open(path)?.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
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
