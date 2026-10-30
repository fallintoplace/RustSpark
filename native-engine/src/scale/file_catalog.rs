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
        crate::parquet_scan::scan_checked(&self.path, batch_rows, &self.schema)
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

#[cfg(test)]
mod replaced_parquet_schema_regressions {
    use super::*;
    use crate::{arrow_bridge, batch::Batch, scalar::Value, schema::{DataType, Field}};
    use parquet::arrow::ArrowWriter;
    use std::{fs, time::{SystemTime, UNIX_EPOCH}};

    fn write_table(path: &Path, field: Field, value: Value) {
        let schema = Schema::new(vec![field]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![value]]).unwrap();
        let arrow = arrow_bridge::to_arrow(&batch).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), arrow.schema(), None).unwrap();
        writer.write(&arrow).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn replaced_file_cannot_change_registered_table_schema() {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rustspark-replaced-parquet-{}-{unique}.parquet", std::process::id()
        ));
        write_table(&path, Field::new("id", DataType::Integer, false), Value::Integer(5));
        let mut catalog = FileCatalog::new();
        catalog.register_parquet("events", &path).unwrap();
        assert_eq!(catalog.get_parquet("events").unwrap().unwrap().open(4).unwrap()
            .next().unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(5)]);

        write_table(&path, Field::new("id", DataType::Text, false), Value::Text("changed".into()));
        let error = catalog.get_parquet("events").unwrap().unwrap().open(4).err().unwrap();
        assert!(error.to_string().contains("schema changed"));
        fs::remove_file(path).unwrap();
    }
}
