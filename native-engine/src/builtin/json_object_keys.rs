use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::{Deserialize, de::{IgnoredAny, MapAccess, Visitor}};
use std::fmt;

struct RootObjectKeys(Vec<String>);

impl<'de> Deserialize<'de> for RootObjectKeys {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error>
    {
        struct Collector;
        impl<'de> Visitor<'de> for Collector {
            type Value = RootObjectKeys;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                let mut keys = Vec::new();
                while let Some((key, _)) = map.next_entry::<String, IgnoredAny>()? {
                    keys.push(key);
                }
                Ok(RootObjectKeys(keys))
            }
        }
        deserializer.deserialize_map(Collector)
    }
}


fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

struct KeyOutput {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for KeyOutput {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("JSON_OBJECT_KEYS output exceeds byte budget"));
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn serialize_keys(map: &serde_json::Map<String, serde_json::Value>, limit: usize) -> Result<String> {
    use serde::ser::{SerializeSeq, Serializer};
    let mut output = KeyOutput { bytes: Vec::new(), limit };
    (|| -> serde_json::Result<()> {
        let mut serializer = serde_json::Serializer::new(&mut output);
        let mut keys = serializer.serialize_seq(Some(map.len()))?;
        for key in map.keys() { keys.serialize_element(key)?; }
        keys.end()
    })().map_err(|error| Error::Execution(format!(
        "JSON_OBJECT_KEYS serialization failed: {error}"
    )))?;
    Ok(String::from_utf8(output.bytes).expect("JSON serializer emits UTF-8"))
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(value) = super::string(&args[1])? else { return Ok(Value::Null); };
        value
    } else { "$" };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    if path == "$" && text.trim_start().starts_with('{') {
        // Only object member names are needed. Skip each member's value
        // without allocating the nested JSON tree.
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let keys = RootObjectKeys::deserialize(&mut deserializer)
            .and_then(|result| deserializer.end().map(|()| result.0));
        let Ok(mut keys) = keys else { return Ok(Value::Null); };
        // serde_json::Map uses sorted unique keys by default. Preserve its
        // duplicate-key and output-order semantics for the streaming path.
        keys.sort_unstable();
        keys.dedup();
        let mut output = KeyOutput { bytes: Vec::new(), limit: 16 * 1024 * 1024 };
        serde_json::to_writer(&mut output, &keys)
            .map_err(|error| Error::Execution(format!(
                "JSON_OBJECT_KEYS serialization failed: {error}"
            )))?;
        return Ok(Value::Text(String::from_utf8(output.bytes)
            .expect("JSON serializer emits UTF-8")));
    }
    let doc = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(doc) => doc,
        Err(_) => return Ok(Value::Null),
    };
    match json_path::lookup(Some(&doc), path)? {
        None | Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::Object(map)) => {
            Ok(Value::Text(serialize_keys(map, 16 * 1024 * 1024)?))
        }
        _ => Ok(Value::Null),
    }
}


fn json_has_key_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}
fn json_has_key(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(key) = super::string(&args[1])? else { return Ok(Value::Null); };
    let doc = json_path::parse(text)?;
    match doc {
        serde_json::Value::Object(fields) => Ok(Value::Boolean(fields.contains_key(key))),
        _ => Err(Error::Type("JSON_HAS_KEY requires an object".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_has_key", min_args: 2, max_args: 2, result_type: json_has_key_type, call: json_has_key },Function {
    name: "json_object_keys", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enumerates_json_keys_as_json_text() {
        assert_eq!(evaluate(&[Value::Text(r#"{"a":1,"b":2}"#.into())]).unwrap(),
            Value::Text(r#"["a","b"]"#.into()));
        assert_eq!(evaluate(&[Value::Text(r#"{"nested":{}}"#.into()),
            Value::Text("$.nested".into())]).unwrap(), Value::Text("[]".into()));
        assert_eq!(evaluate(&[Value::Text("[]".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text(r#"{"x":1,"y":2,"z":3}"#.into())]).unwrap(),
            Value::Text(r#"["x","y","z"]"#.into()));
    }
}

#[cfg(test)]
mod json_has_key_extension_tests {
    use super::*;
    #[test]
    fn handles_json_values_and_missing_data() {
        assert!(crate::builtin::supports("json_has_key"));
        let doc = Value::Text(r#"{"x":null,"y":1}"#.into());
        assert_eq!(json_has_key(&[doc.clone(), Value::Text("x".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(json_has_key(&[doc, Value::Text("z".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(json_has_key(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
        assert!(json_has_key(&[Value::Text("[]".into()), Value::Text("x".into())]).is_err());
    }
}

#[cfg(test)]
mod invalid_json_object_keys_tests {
    use super::*;
    #[test]
    fn non_object_or_malformed_json_produces_null() {
        for text in ["[]", "7", "{bad", ""] {
            assert_eq!(evaluate(&[Value::Text(text.into())]).unwrap(), Value::Null);
        }
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_object_keys_budget_tests {
    use super::*;
    #[test]
    fn serializes_key_order_and_stops_before_budget() {
        let document = serde_json::json!({"a":1,"long":2});
        let map = document.as_object().unwrap();
        assert_eq!(serialize_keys(map, 100).unwrap(), "[\"a\",\"long\"]");
        assert!(serialize_keys(map, 8).is_err());
        let document = serde_json::json!({"\u{0000}\u{0000}\u{0000}": 1});
        assert!(serialize_keys(document.as_object().unwrap(), 12).is_err());
    }
}

#[cfg(test)]
mod streaming_root_object_keys_tests {
    use super::*;

    #[test]
    fn root_keys_are_sorted_deduplicated_and_preserve_unicode() {
        let input = r#"{"z":{"deep":[1,2,3]},"é":null,"a":true,"z":4}"#;
        assert_eq!(evaluate(&[Value::Text(input.into())]).unwrap(),
            Value::Text(r#"["a","z","é"]"#.into()));
        let wide = format!(r#"{{"payload":[{}],"name":"🙂"}}"#,
            vec!["{\"x\":[1,null,true]}"; 10_000].join(","));
        assert_eq!(evaluate(&[Value::Text(wide)]).unwrap(),
            Value::Text(r#"["name","payload"]"#.into()));
        assert_eq!(evaluate(&[Value::Text("{}".into())]).unwrap(),
            Value::Text("[]".into()));
    }

    #[test]
    fn malformed_root_returns_null_and_nested_paths_still_work() {
        for invalid in ["{", "{\"a\": 1} trailing", "{\"a\": [1,INVALID]}"] {
            assert_eq!(evaluate(&[Value::Text(invalid.into())]).unwrap(),
                Value::Null);
        }
        assert_eq!(evaluate(&[Value::Text(r#"{"outer":{"b":1,"a":2}}"#.into()),
            Value::Text("$.outer".into())]).unwrap(),
            Value::Text(r#"["a","b"]"#.into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
