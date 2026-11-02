//! Incremental, typed JSON Lines decoding without whole-file materialization.
use std::{io::{BufRead, BufReader, Read}, fs::File, path::Path};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value as Json;

use crate::{batch::Batch, scalar::Value, schema::{DataType, Schema}, Error, Result};

const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATCH_ROWS: usize = 16_384;
const MAX_CELLS_PER_BATCH: usize = 1_000_000;

/// The consumer supplies a schema to avoid buffering every input row just to
/// infer column types. Each next() holds only one JSON line and one row batch.
pub struct JsonlScan<R> {
    reader: R,
    schema: Schema,
    batch_rows: usize,
    max_total_bytes: usize,
    bytes_read: usize,
    line: String,
    finished: bool,
}

// Unlike serde_json::Map's default decoding, this rejects duplicate keys.
struct UniqueRow(Vec<(String, Json)>);

impl<'de> Deserialize<'de> for UniqueRow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct RowVisitor;
        impl<'de> Visitor<'de> for RowVisitor {
            type Value = UniqueRow;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }
            fn visit_map<A: MapAccess<'de>>(
                self, mut access: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut columns = Vec::new();
                let mut seen = std::collections::HashSet::new();
                while let Some((name, value)) = access.next_entry::<String, Json>()? {
                    if !seen.insert(name.clone()) {
                        return Err(de::Error::custom("duplicate JSONL field"));
                    }
                    columns.push((name, value));
                }
                Ok(UniqueRow(columns))
            }
        }
        deserializer.deserialize_map(RowVisitor)
    }
}

impl<R: BufRead> JsonlScan<R> {
    pub fn new(
        reader: R, schema: Schema, batch_rows: usize, max_total_bytes: usize,
    ) -> Result<Self> {
        if batch_rows == 0 || batch_rows > MAX_BATCH_ROWS
            || batch_rows.checked_mul(schema.len().max(1))
                .is_none_or(|cells| cells > MAX_CELLS_PER_BATCH) {
            return Err(Error::Execution("JSONL streaming batch budget exceeded".into()));
        }
        Ok(Self {
            reader, schema, batch_rows, max_total_bytes,
            bytes_read: 0, line: String::new(), finished: false,
        })
    }

    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn bytes_read(&self) -> usize { self.bytes_read }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        loop {
            self.line.clear();
            let remaining = self.max_total_bytes.saturating_sub(self.bytes_read);
            if remaining == 0 {
                if self.reader.fill_buf()?.is_empty() { return Ok(None); }
                return Err(Error::Execution("JSONL stream byte budget exceeded".into()));
            }
            // A bounded Take reader stops an untrusted line from allocating
            // beyond MAX_LINE_BYTES or the entire remaining input budget.
            let limit = remaining.min(MAX_LINE_BYTES).saturating_add(1);
            let read = (&mut self.reader).take(limit as u64).read_line(&mut self.line)?;
            if read == 0 { return Ok(None); }
            if read > MAX_LINE_BYTES || read > remaining
                || (read == limit && !self.line.ends_with('\n')) {
                return Err(Error::Execution("JSONL line or stream byte budget exceeded".into()));
            }
            self.bytes_read += read;
            if self.line.trim().is_empty() { continue; }

            let UniqueRow(fields) = serde_json::from_str::<UniqueRow>(&self.line)
                .map_err(|error| Error::Parse(format!("invalid JSONL record: {error}")))?;
            let mut row = vec![Value::Null; self.schema.len()];
            let mut seen = vec![false; self.schema.len()];
            for (name, json) in fields {
                let index = self.schema.column(&name)?;
                if seen[index] {
                    return Err(Error::Schema("duplicate JSONL field ignoring case".into()));
                }
                seen[index] = true;
                let value = match json {
                    Json::Null => Value::Null,
                    Json::Bool(value) => Value::Boolean(value),
                    Json::String(value) => Value::Text(value),
                    Json::Number(value) if value.as_i64().is_some() =>
                        Value::Integer(value.as_i64().expect("checked signed number")),
                    Json::Number(value) if value.as_u64().is_some() =>
                        return Err(Error::Type("JSONL unsigned integer exceeds BIGINT range".into())),
                    Json::Number(value) => Value::Float(value.as_f64()
                        .filter(|n| n.is_finite())
                        .ok_or_else(|| Error::Type("invalid JSONL number".into()))?),
                    _ => return Err(Error::Unsupported(
                        "native JSONL supports scalar values only".into())),
                };
                let target = self.schema.field(index)?.data_type;
                row[index] = if target == DataType::Float && matches!(value, Value::Integer(_)) {
                    value.cast_to(DataType::Float)?
                } else if target.accepts(&value) {
                    value
                } else {
                    return Err(Error::Type(format!("incompatible JSONL column: {name}")));
                };
            }
            return Ok(Some(row));
        }
    }
}

impl<R: BufRead> Iterator for JsonlScan<R> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        let mut rows = Vec::new();
        if let Err(error) = rows.try_reserve_exact(self.batch_rows) {
            self.finished = true;
            return Some(Err(Error::Execution(format!("JSONL batch allocation failed: {error}"))));
        }
        while rows.len() < self.batch_rows {
            match self.next_row() {
                Ok(Some(row)) => rows.push(row),
                Ok(None) => { self.finished = true; break; }
                Err(error) => { self.finished = true; return Some(Err(error)); }
            }
        }
        if rows.is_empty() {
            return None;
        }
        let result = Batch::from_rows(self.schema.clone(), rows);
        if result.is_err() { self.finished = true; }
        Some(result)
    }
}

/// Open a regular local JSONL file as an incremental, typed reader.
pub fn scan_jsonl(
    path: impl AsRef<Path>, schema: Schema, batch_rows: usize, max_total_bytes: usize,
) -> Result<JsonlScan<BufReader<File>>> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Execution("JSONL source must be a regular file".into()));
    }
    JsonlScan::new(BufReader::new(file), schema, batch_rows, max_total_bytes)
}
