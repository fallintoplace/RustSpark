use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result};

// Keep only the last value of the requested root member. Every skipped
// subtree is still parsed to reject corrupt JSON after an early match.
struct RootMemberExtract<'a>(&'a str);

impl<'de> serde::de::DeserializeSeed<'de> for RootMemberExtract<'_> {
    type Value = Option<serde_json::Value>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self, deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        struct Selector<'a>(&'a str);
        impl<'de> serde::de::Visitor<'de> for Selector<'_> {
            type Value = Option<serde_json::Value>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self, mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut selected = None;
                while let Some(name) = map.next_key::<String>()? {
                    if name == self.0 {
                        selected = Some(map.next_value::<serde_json::Value>()?);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(selected)
            }
        }
        deserializer.deserialize_map(Selector(self.0))
    }
}

fn direct_root_member(path: &str) -> Option<&str> {
    let name = path.strip_prefix("$.")?;
    (path.len() <= 4096 && !name.is_empty() &&
        name.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_'))
        .then_some(name)
}

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
        path
    } else { "$" };
    if let Some(name) = direct_root_member(path)
        && text.trim_start().starts_with('{')
    {
        use serde::de::DeserializeSeed;
        if text.len() > 16 * 1024 * 1024 {
            return Err(crate::Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let selected = RootMemberExtract(name).deserialize(&mut deserializer)
            .and_then(|value| deserializer.end().map(|()| value))
            .map_err(|error| crate::Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return selected.map_or(Ok(Value::Null), |value| Ok(Value::Text(
            json_path::serialize_bounded(&value, 16 * 1024 * 1024)?
        )));
    }
    let doc = json_path::parse(text)?;
    match json_path::lookup(Some(&doc), path)? {
        Some(value) => Ok(Value::Text(json_path::serialize_bounded(value, 16 * 1024 * 1024)?)),
        None => Ok(Value::Null),
    }
}


// Keep only the selected root array element. Other elements still undergo
// full syntax validation, but do not allocate serde_json::Value subtrees.
struct RootArrayItem(Option<usize>);

impl<'de> serde::de::DeserializeSeed<'de> for RootArrayItem {
    type Value = Option<serde_json::Value>;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D)
        -> std::result::Result<Self::Value, D::Error>
    {
        struct ItemVisitor(Option<usize>);
        impl<'de> serde::de::Visitor<'de> for ItemVisitor {
            type Value = Option<serde_json::Value>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON array")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                let mut selected = None;
                let mut index = 0usize;
                loop {
                    if self.0 == Some(index) {
                        let Some(value) = seq.next_element::<serde_json::Value>()? else { break; };
                        selected = Some(value);
                    } else if seq.next_element::<serde::de::IgnoredAny>()?.is_none() {
                        break;
                    }
                    index += 1;
                }
                Ok(selected)
            }
        }
        deserializer.deserialize_seq(ItemVisitor(self.0))
    }
}

fn json_array_at_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(crate::Error::Type("JSON_ARRAY_AT expects JSON text and integer index".into()))
    }
}
fn json_array_at(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(index) = super::integer(&args[1])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(crate::Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    if text.trim_start().starts_with('[') {
        use serde::de::DeserializeSeed;
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let value = RootArrayItem(usize::try_from(index).ok())
            .deserialize(&mut deserializer)
            .and_then(|value| deserializer.end().map(|()| value))
            .map_err(|error| crate::Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return match value {
            Some(value) => Ok(Value::Text(json_path::serialize_bounded(&value, 16 * 1024 * 1024)?)),
            None => Ok(Value::Null),
        };
    }
    let doc = json_path::parse(text)?;
    match doc {
        serde_json::Value::Array(items) => match usize::try_from(index).ok()
            .and_then(|offset| items.get(offset)) {
            Some(item) => Ok(Value::Text(json_path::serialize_bounded(item, 16 * 1024 * 1024)?)),
            None => Ok(Value::Null),
        },
        _ => Err(crate::Error::Type("JSON_ARRAY_AT requires an array".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_array_at", min_args: 2, max_args: 2, result_type: json_array_at_type, call: json_array_at },Function {
    name: "json_extract", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_canonical_json_including_literal_null() {
        let doc = Value::Text(r#"{"n":4,"missing":null}"#.into());
        assert_eq!(evaluate(&[doc.clone(), Value::Text("$.n".into())]).unwrap(),
            Value::Text("4".into()));
        assert_eq!(evaluate(&[doc.clone(), Value::Text("$.missing".into())]).unwrap(),
            Value::Text("null".into()));
        assert_eq!(evaluate(&[doc, Value::Text("$.absent".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_array_at_extension_tests {
    use super::*;
    #[test]
    fn handles_json_values_and_missing_data() {
        assert!(crate::builtin::supports("json_array_at"));
        let arr = Value::Text(r#"[1,null,{"x":2}]"#.into());
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(0)]).unwrap(), Value::Text("1".into()));
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(1)]).unwrap(), Value::Text("null".into()));
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(json_array_at(&[arr, Value::Integer(7)]).unwrap(), Value::Null);
        assert_eq!(json_array_at(&[Value::Null, Value::Integer(0)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod root_array_item_streaming_tests {
    use super::*;

    #[test]
    fn selects_one_element_from_a_wide_array() {
        let ignored = vec![r#"{"nested":[1,2,{"x":"🙂"}]}"#; 12_000].join(",");
        let input = format!("[{ignored},{{\"target\":[null,true]}}]");
        assert_eq!(json_array_at(&[
            Value::Text(input), Value::Integer(12_000),
        ]).unwrap(), Value::Text(r#"{"target":[null,true]}"#.into()));
    }

    #[test]
    fn matches_materialized_reference_and_validates_all_elements() {
        let json = r#"[null,1.0,{"x":[1,2]},"é🙂"]"#;
        let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
        let values = parsed.as_array().unwrap();
        for index in [-1, 0, 1, 2, 3, 4, i64::MAX] {
            let expected = usize::try_from(index).ok().and_then(|n| values.get(n))
                .map(|value| Value::Text(serde_json::to_string(value).unwrap()))
                .unwrap_or(Value::Null);
            assert_eq!(json_array_at(&[
                Value::Text(json.into()), Value::Integer(index),
            ]).unwrap(), expected, "index={index}");
        }
        for input in ["[1,2] true", "[1,2,", "[1,,2]"] {
            assert!(matches!(json_array_at(&[
                Value::Text(input.into()), Value::Integer(0),
            ]), Err(crate::Error::Execution(_))));
        }
        assert!(matches!(json_array_at(&[
            Value::Text("{}".into()), Value::Integer(0),
        ]), Err(crate::Error::Type(_))));
    }
}

#[cfg(test)]
mod streamed_json_root_extraction_tests {
    use super::*;

    #[test]
    fn matches_dom_serialization_for_duplicate_and_unicode_members() {
        for (document, path) in [
            (r#"{"target":[1,{"x":true},null],"skip":{"wide":[0,1]}}"#, "$.target"),
            (r#"{"target":null}"#, "$.target"),
            (r#"{"target":"é🙂"}"#, "$.target"),
            (r#"{"target":1,"target":{"x":9}}"#, "$.target"),
            (r#"{"t\u0061rget":true}"#, "$.target"),
            (r#"{"other":{"x":[true,false]}}"#, "$.target"),
            (r#"{"nested":{"target":[1,2]}}"#, "$.nested.target"),
        ] {
            let doc = json_path::parse(document).unwrap();
            let expected = match json_path::lookup(Some(&doc), path).unwrap() {
                Some(value) => Value::Text(
                    json_path::serialize_bounded(value, 16 * 1024 * 1024).unwrap()),
                None => Value::Null,
            };
            assert_eq!(evaluate(&[
                Value::Text(document.into()), Value::Text(path.into()),
            ]).unwrap(), expected, "{document} {path}");
        }
    }

    #[test]
    fn refuses_invalid_trailing_json_after_selected_member() {
        for invalid in [
            r#"{"target":[1,2],"extra":[1,,2]}"#,
            r#"{"target":1} trailing"#,
            r#"{"missing":"unterminated}"#,
        ] {
            assert!(evaluate(&[
                Value::Text(invalid.into()), Value::Text("$.target".into()),
            ]).is_err());
        }
    }
}
