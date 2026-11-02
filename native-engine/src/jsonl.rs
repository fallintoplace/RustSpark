//! Read bounded scalar JSON Lines into native typed SQL batches.
use std::{collections::BTreeSet, fs::File, io::Read, path::Path};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value as Json};
use crate::{batch::Batch, scalar::Value, schema::{DataType, Field, Schema}, Error, Result};

const MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;
const MAX_ROWS: usize = 100_000;
const MAX_COLUMNS: usize = 4096;
const MAX_CELLS: usize = 2_000_000;

// serde_json::Value overwrites duplicate object keys. Reject them at decoding
// time so a malformed input cannot silently change the values in a table.
struct UniqueObject(Map<String, Json>);

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct UniqueObjectVisitor;
        impl<'de> Visitor<'de> for UniqueObjectVisitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON object containing unique keys")
            }

            fn visit_map<A: MapAccess<'de>>(
                self, mut access: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut record = Map::new();
                while let Some((name, value)) = access.next_entry::<String, Json>()? {
                    if record.contains_key(&name) {
                        return Err(de::Error::custom("duplicate JSONL key"));
                    }
                    record.insert(name, value);
                }
                Ok(UniqueObject(record))
            }
        }
        deserializer.deserialize_map(UniqueObjectVisitor)
    }
}

enum NativeNumber {
    Integer(i64),
    Float(f64),
}

fn native_number(number: &serde_json::Number) -> Result<NativeNumber> {
    if let Some(value) = number.as_i64() {
        return Ok(NativeNumber::Integer(value));
    }
    if number.as_u64().is_some() {
        return Err(Error::Type("JSONL unsigned integer exceeds BIGINT range".into()));
    }
    number.as_f64().filter(|n| n.is_finite())
        .map(NativeNumber::Float)
        .ok_or_else(|| Error::Type("invalid JSONL number".into()))
}

fn native_type(value: &Json) -> Result<Option<DataType>> {
    match value {
        Json::Null => Ok(None),
        Json::Bool(_) => Ok(Some(DataType::Boolean)),
        Json::String(_) => Ok(Some(DataType::Text)),
        Json::Number(number) => Ok(Some(match native_number(number)? {
            NativeNumber::Integer(_) => DataType::Integer,
            NativeNumber::Float(_) => DataType::Float,
        })),
        _ => Err(Error::Unsupported("native JSONL supports scalar values only".into())),
    }
}

fn native_value(value: Json) -> Result<Value> {
    match value {
        Json::Null => Ok(Value::Null),
        Json::Bool(flag) => Ok(Value::Boolean(flag)),
        Json::String(text) => Ok(Value::Text(text)),
        Json::Number(number) => Ok(match native_number(&number)? {
            NativeNumber::Integer(value) => Value::Integer(value),
            NativeNumber::Float(value) => Value::Float(value),
        }),
        _ => Err(Error::Unsupported("native JSONL supports scalar values only".into())),
    }
}

pub fn from_jsonl_str(input: &str) -> Result<Batch> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(Error::Execution("JSONL input exceeds 32 MiB limit".into()));
    }
    let mut records = Vec::<Map<String, Json>>::new();
    let mut names = BTreeSet::<String>::new();
    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        if records.len() >= MAX_ROWS {
            return Err(Error::Execution("JSONL row limit exceeded".into()));
        }
        let UniqueObject(record) = serde_json::from_str::<UniqueObject>(line)
            .map_err(|e| Error::Parse(format!("invalid JSONL record: {e}")))?;
        for (name, value) in &record {
            if name.is_empty() { return Err(Error::Schema("empty JSONL column".into())); }
            native_type(value)?;
            names.insert(name.clone());
            if names.len() > MAX_COLUMNS {
                return Err(Error::Execution("JSONL column limit exceeded".into()));
            }
        }
        records.push(record);
    }
    records.len().checked_mul(names.len())
        .filter(|&count| count <= MAX_CELLS)
        .ok_or_else(|| Error::Execution("JSONL cell budget exceeded".into()))?;
    let mut fields = Vec::new();
    for name in &names {
        let mut kind = None;
        let mut nullable = false;
        for record in &records {
            let next = record.get(name).map(native_type).transpose()?.flatten();
            match (kind, next) {
                (_, None) => nullable = true,
                (None, Some(value)) => kind = Some(value),
                (Some(DataType::Integer), Some(DataType::Float)) |
                (Some(DataType::Float), Some(DataType::Integer)) => kind = Some(DataType::Float),
                (Some(a), Some(b)) if a == b => {},
                _ => return Err(Error::Type(format!("incompatible JSONL column: {name}"))),
            }
        }
        fields.push(Field::new(name, kind.unwrap_or(DataType::Text), nullable));
    }
    let schema = Schema::new(fields)?;
    let mut rows = Vec::new();
    for mut record in records {
        let mut row = Vec::new();
        for field in schema.fields() {
            let mut value = native_value(record.remove(&field.name).unwrap_or(Json::Null))?;
            if !field.data_type.accepts(&value) {
                value = value.cast_to(field.data_type)?;
            }
            row.push(value);
        }
        rows.push(row);
    }
    Batch::from_rows(schema, rows)
}

pub fn read_jsonl(path: impl AsRef<Path>) -> Result<Batch> {
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Execution("JSONL source must be a regular file".into()));
    }
    if file.metadata()?.len() > MAX_INPUT_BYTES as u64 {
        return Err(Error::Execution("JSONL input exceeds 32 MiB limit".into()));
    }
    let mut text = String::new();
    file.take(MAX_INPUT_BYTES as u64 + 1).read_to_string(&mut text)?;
    from_jsonl_str(&text)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sparse_unicode_rows_widen_numeric_types_and_keep_nulls() {
        let batch = from_jsonl_str(
            "{\"id\":1,\"score\":1,\"name\":\"é🙂\"}\n\
             {\"id\":2,\"score\":2.5}\n{\"id\":3,\"name\":null}\n"
        ).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema().len(), 3);
        assert_eq!(batch.value(0, batch.schema().column("score").unwrap()).unwrap(),
            &Value::Float(1.0));
        assert_eq!(batch.value(1, batch.schema().column("score").unwrap()).unwrap(),
            &Value::Float(2.5));
        assert_eq!(batch.value(2, batch.schema().column("score").unwrap()).unwrap(),
            &Value::Null);
    }
    #[test]
    fn rejects_nested_values_conflicting_types_and_oversized_integers() {
        for text in [
            "{\"id\":[]}", "{\"id\":{}}", "{\"id\":1}\n{\"id\":true}",
            "[1,2]", "{\"id\":9223372036854775808}", "{\"\":1}",
            "{\"id\":1,\"id\":2}"
        ] {
            assert!(from_jsonl_str(text).is_err(), "{text}");
        }
    }
}
