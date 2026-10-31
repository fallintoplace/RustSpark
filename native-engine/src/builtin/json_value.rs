use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result, Error};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

// Materialize only scalar root values. Container children are validated
// and discarded, which avoids retaining wide trees just to reject them.
enum RootScalar {
    Null,
    Text(String),
    Container,
}

impl<'de> serde::Deserialize<'de> for RootScalar {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error>
    {
        struct ScalarVisitor;
        impl<'de> serde::de::Visitor<'de> for ScalarVisitor {
            type Value = RootScalar;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON scalar or container")
            }

            fn visit_unit<E: serde::de::Error>(self)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Null)
            }

            fn visit_bool<E: serde::de::Error>(self, value: bool)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Text(value.to_string()))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Text(serde_json::Number::from(value).to_string()))
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Text(serde_json::Number::from(value).to_string()))
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64)
                -> std::result::Result<Self::Value, E>
            {
                let number = serde_json::Number::from_f64(value)
                    .expect("a parsed JSON number is finite");
                Ok(RootScalar::Text(number.to_string()))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Text(value.to_owned()))
            }

            fn visit_string<E: serde::de::Error>(self, value: String)
                -> std::result::Result<Self::Value, E>
            {
                Ok(RootScalar::Text(value))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(RootScalar::Container)
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                while map.next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                    .is_some() {}
                Ok(RootScalar::Container)
            }
        }
        deserializer.deserialize_any(ScalarVisitor)
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    if path == "$" {
        use serde::Deserialize;
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let root = RootScalar::deserialize(&mut deserializer)
            .and_then(|root| deserializer.end().map(|()| root))
            .map_err(|error| Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return match root {
            RootScalar::Null => Ok(Value::Null),
            RootScalar::Text(value) => Ok(Value::Text(value)),
            RootScalar::Container => Err(Error::Type(
                "JSON_VALUE requires a scalar JSON node".into()
            )),
        };
    }
    let document = json_path::parse(text)?;
    let Some(value) = json_path::lookup(Some(&document), path)? else { return Ok(Value::Null); };
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::String(value) => Ok(Value::Text(value.clone())),
        serde_json::Value::Bool(value) => Ok(Value::Text(value.to_string())),
        serde_json::Value::Number(value) => Ok(Value::Text(value.to_string())),
        _ => Err(Error::Type("JSON_VALUE requires a scalar JSON node".into())),
    }
}


struct PatchOutput {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for PatchOutput {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if chunk.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("JSON_MERGE_PATCH output budget exceeded"));
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn json_merge_patch_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn apply_merge_patch(target: &mut serde_json::Value, patch: serde_json::Value) {
    match patch {
        serde_json::Value::Object(updates) => {
            if !target.is_object() { *target = serde_json::Value::Object(serde_json::Map::new()); }
            let fields = target.as_object_mut().expect("target was normalized to object");
            for (key, value) in updates {
                if value.is_null() {
                    fields.remove(&key);
                } else {
                    // A parsed patch is already owned. Move its keys and
                    // replacement subtrees instead of recursively cloning them.
                    apply_merge_patch(fields.entry(key).or_insert(serde_json::Value::Null), value);
                }
            }
        }
        replacement => *target = replacement,
    }
}
fn json_merge_patch(args: &[Value]) -> Result<Value> {
    let Some(document) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(patch) = super::string(&args[1])? else { return Ok(Value::Null); };
    let mut document = json_path::parse(document)?;
    let patch = json_path::parse(patch)?;
    apply_merge_patch(&mut document, patch);
    let mut writer = PatchOutput { bytes: Vec::new(), limit: 16 * 1024 * 1024 };
    serde_json::to_writer(&mut writer, &document)
        .map_err(|error| Error::Execution(format!("JSON_MERGE_PATCH output failed: {error}")))?;
    Ok(Value::Text(String::from_utf8(writer.bytes)
        .expect("serde_json writes UTF-8")))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_merge_patch", min_args: 2, max_args: 2, result_type: json_merge_patch_type, call: json_merge_patch },Function {
    name: "json_value", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::Catalog, executor, batch::Batch, schema::{Field, Schema}, sql};
    #[test]
    fn extracts_nested_scalar_from_native_sql() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("doc", DataType::Text, false)]).unwrap();
        let data = Batch::from_rows(schema, vec![
            vec![Value::Text(r#"{"customer":{"id":"abc"}}"#.into())],
        ]).unwrap();
        catalog.register("t", data).unwrap();
        let rows = executor::execute(&sql::parse_sql(
            "SELECT JSON_VALUE(doc, '$.customer.id') AS customer FROM t"
        ).unwrap(), &catalog).unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Text("abc".into())]]);
        assert!(evaluate(&[
            Value::Text(r#"{"x":[1]}"#.into()), Value::Text("$.x".into())
        ]).is_err());
    }
}

#[cfg(test)]
mod json_merge_patch_extension_tests {
    use super::*;
    #[test]
    fn handles_special_values_and_nulls() {
        assert!(crate::builtin::supports("json_merge_patch"));
        let actual = json_merge_patch(&[
            Value::Text(r#"{"a":1,"b":{"c":2,"d":3}}"#.into()),
            Value::Text(r#"{"a":null,"b":{"c":5},"e":9}"#.into()),
        ]).unwrap();
        let Value::Text(result) = actual else { panic!("expected text"); };
        assert_eq!(serde_json::from_str::<serde_json::Value>(&result).unwrap(),
            serde_json::json!({"b":{"c":5,"d":3},"e":9}));
        assert_eq!(json_merge_patch(&[Value::Text("[]".into()), Value::Text(r#"{"x":1}"#.into())]).unwrap(), Value::Text(r#"{"x":1}"#.into()));
        assert_eq!(json_merge_patch(&[Value::Null, Value::Text("{}".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_patch_bounded_output_tests {
    use super::*;
    #[test]
    fn writer_rejects_growth_and_patch_stays_correct() {
        let mut writer = PatchOutput { bytes: Vec::new(), limit: 6 };
        assert!(serde_json::to_writer(&mut writer, &serde_json::json!({"long":"value"})).is_err());
        assert!(writer.bytes.len() <= 6);
        let value = json_merge_patch(&[Value::Text(r#"{"a":1}"#.into()),
            Value::Text(r#"{"b":2}"#.into())]).unwrap();
        assert_eq!(value, Value::Text(r#"{"a":1,"b":2}"#.into()));
    }
}

#[cfg(test)]
mod owned_json_merge_patch_regressions {
    use super::*;

    #[test]
    fn large_nested_arrays_are_moved_into_target_without_semantic_changes() {
        let values = (0..10_000_i64).map(|index|
            serde_json::json!({"n": index, "label": format!("item-{index}")})
        ).collect::<Vec<_>>();
        let patch = serde_json::json!({
            "events": values,
            "nested": {"remove": null, "add": [true, false]}
        });
        let mut original = serde_json::json!({
            "nested": {"remove": "old", "keep": 9},
            "unchanged": "stable"
        });
        apply_merge_patch(&mut original, patch.clone());
        assert_eq!(original["events"], patch["events"]);
        assert_eq!(original["nested"]["keep"], serde_json::json!(9));
        assert!(original["nested"].get("remove").is_none());
        assert_eq!(original["nested"]["add"], serde_json::json!([true, false]));
        assert_eq!(original["unchanged"], serde_json::json!("stable"));
    }

    #[test]
    fn non_object_patch_replaces_root_and_object_patch_materializes_it() {
        let mut root = serde_json::json!({"old": true});
        apply_merge_patch(&mut root, serde_json::json!([1, 2, 3]));
        assert_eq!(root, serde_json::json!([1, 2, 3]));
        apply_merge_patch(&mut root, serde_json::json!({"field": "new"}));
        assert_eq!(root, serde_json::json!({"field": "new"}));
        apply_merge_patch(&mut root, serde_json::Value::Null);
        assert!(root.is_null());
    }
}

#[cfg(test)]
mod root_json_value_streaming_tests {
    use super::*;

    #[test]
    fn matches_serde_scalar_text_for_every_root_shape() {
        for input in ["null", "true", "false", "0", "-0", "1.0", "-2.4e+3",
            "18446744073709551615", r#""café\n🙂""#] {
            let parsed: serde_json::Value = serde_json::from_str(input).unwrap();
            let expected = match parsed {
                serde_json::Value::Null => Value::Null,
                serde_json::Value::Bool(value) => Value::Text(value.to_string()),
                serde_json::Value::Number(value) => Value::Text(value.to_string()),
                serde_json::Value::String(value) => Value::Text(value),
                _ => unreachable!(),
            };
            assert_eq!(evaluate(&[
                Value::Text(input.into()), Value::Text("$".into()),
            ]).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn does_not_accept_containers_or_malformed_suffixes() {
        let many = format!("[{}]", vec![r#"{"a":[1,2,null]}"#; 5_000].join(","));
        assert!(matches!(evaluate(&[
            Value::Text(many), Value::Text("$".into()),
        ]), Err(Error::Type(_))));
        for input in [r#"{"a":1} trailing"#, "[1,,2]", r#""unterminated"#] {
            assert!(matches!(evaluate(&[
                Value::Text(input.into()), Value::Text("$".into()),
            ]), Err(Error::Execution(_))));
        }
        assert_eq!(evaluate(&[
            Value::Text(r#"{"a":"yes"}"#.into()), Value::Text("$.a".into()),
        ]).unwrap(), Value::Text("yes".into()));
    }
}
