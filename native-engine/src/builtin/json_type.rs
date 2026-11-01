use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::{Deserialize, de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor}};
use std::{collections::HashMap, fmt};

// Root JSON_TYPE only needs the shape, not an allocated value tree.
struct RootJsonType(&'static str);

impl<'de> Deserialize<'de> for RootJsonType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error>
    {
        struct Classifier;

        impl<'de> Visitor<'de> for Classifier {
            type Value = RootJsonType;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("NULL"))
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("BOOLEAN"))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("NUMBER"))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("NUMBER"))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("NUMBER"))
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
                Ok(RootJsonType("STRING"))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(RootJsonType("ARRAY"))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                while map.next_entry::<String, IgnoredAny>()?.is_some() {}
                Ok(RootJsonType("OBJECT"))
            }
        }
        deserializer.deserialize_any(Classifier)
    }
}


// Only materialize the shape of the selected member. Last-write-wins
// matches serde_json::Map for duplicate object keys.
struct RootMemberType<'a>(&'a str);

impl<'de> DeserializeSeed<'de> for RootMemberType<'_> {
    type Value = Option<RootJsonType>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self, deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        struct Selector<'a>(&'a str);
        impl<'de> Visitor<'de> for Selector<'_> {
            type Value = Option<RootJsonType>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(
                self, mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut selected = None;
                while let Some(name) = map.next_key::<String>()? {
                    if name == self.0 {
                        selected = Some(map.next_value::<RootJsonType>()?);
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(selected)
            }
        }
        deserializer.deserialize_map(Selector(self.0))
    }
}

fn direct_root_member(path: &str) -> Option<&str> {
    let member = path.strip_prefix("$.")?;
    (path.len() <= 4096 && !member.is_empty() &&
        member.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_'))
        .then_some(member)
}

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
        path
    } else { "$" };
    if path == "$" {
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let RootJsonType(label) = RootJsonType::deserialize(&mut deserializer)
            .and_then(|value| deserializer.end().map(|()| value))
            .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))?;
        return Ok(Value::Text(label.into()));
    }
    if let Some(name) = direct_root_member(path)
        && text.trim_start().starts_with('{')
    {
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let selected = RootMemberType(name).deserialize(&mut deserializer)
            .and_then(|selected| deserializer.end().map(|()| selected))
            .map_err(|error| Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return Ok(selected.map_or(Value::Null,
            |RootJsonType(label)| Value::Text(label.into())));
    }
    let document = json_path::parse(text)?;
    let Some(found) = json_path::lookup(Some(&document), path)? else { return Ok(Value::Null); };
    let label = match found {
        serde_json::Value::Null => "NULL",
        serde_json::Value::Bool(_) => "BOOLEAN",
        serde_json::Value::Number(_) => "NUMBER",
        serde_json::Value::String(_) => "STRING",
        serde_json::Value::Array(_) => "ARRAY",
        serde_json::Value::Object(_) => "OBJECT",
    };
    Ok(Value::Text(label.into()))
}


// Depth only depends on the shape of each value. Keep per-object field
// depths to preserve serde_json::Map's last-write-wins duplicate-key rule.
struct StreamingJsonDepth(usize);

impl<'de> Deserialize<'de> for StreamingJsonDepth {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error>
    {
        struct DepthVisitor;
        impl<'de> Visitor<'de> for DepthVisitor {
            type Value = StreamingJsonDepth;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
                Ok(StreamingJsonDepth(1))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                let mut depth = 1usize;
                while let Some(child) = seq.next_element::<StreamingJsonDepth>()? {
                    depth = depth.max(child.0 + 1);
                }
                Ok(StreamingJsonDepth(depth))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                let mut members = HashMap::<String, usize>::new();
                while let Some((name, child)) =
                    map.next_entry::<String, StreamingJsonDepth>()?
                {
                    members.insert(name, child.0);
                }
                Ok(StreamingJsonDepth(
                    members.into_values().max().unwrap_or(0) + 1
                ))
            }
        }
        deserializer.deserialize_any(DepthVisitor)
    }
}

fn json_depth_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn json_depth(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let depth = StreamingJsonDepth::deserialize(&mut deserializer)
        .and_then(|result| deserializer.end().map(|()| result.0))
        .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))?;
    Ok(Value::Integer(i64::try_from(depth)
        .map_err(|_| Error::Execution("JSON_DEPTH overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_depth", min_args: 1, max_args: 1, result_type: json_depth_type, call: json_depth },Function {
    name: "json_type", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inspects_json_node_types_and_missing_paths() {
        assert_eq!(evaluate(&[Value::Text(r#"{"a":[4]}"#.into()), Value::Text("$.a".into())])
            .unwrap(), Value::Text("ARRAY".into()));
        assert_eq!(evaluate(&[Value::Text("null".into())]).unwrap(), Value::Text("NULL".into()));
        assert_eq!(evaluate(&[Value::Text("{}".into()), Value::Text("$.missing".into())])
            .unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_depth_extension_tests {
    use super::*;
    #[test]
    fn handles_json_values_and_missing_data() {
        assert!(crate::builtin::supports("json_depth"));
        assert_eq!(json_depth(&[Value::Text(r#"{"a":[{"b":1}]}"#.into())]).unwrap(), Value::Integer(4));
        assert_eq!(json_depth(&[Value::Text("{}".into())]).unwrap(), Value::Integer(1));
        assert_eq!(json_depth(&[Value::Text("null".into())]).unwrap(), Value::Integer(1));
        assert_eq!(json_depth(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_depth_scalar_fast_path_tests {
    use super::*;
    #[test]
    fn scalar_roots_have_depth_one_and_objects_still_traverse() {
        for input in ["null", "true", "5", r#""café""#] {
            assert_eq!(json_depth(&[Value::Text(input.into())]).unwrap(), Value::Integer(1));
        }
        assert_eq!(json_depth(&[Value::Text("[[1]]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(json_depth(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod wide_json_depth_tests {
    use super::*;

    #[test]
    fn flat_wide_arrays_and_nested_containers_keep_correct_depth() {
        let wide = format!("[{}]", vec!["null"; 20_000].join(","));
        assert_eq!(json_depth(&[Value::Text(wide)]).unwrap(), Value::Integer(2));
        assert_eq!(json_depth(&[Value::Text("[[1],true]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(json_depth(&[Value::Text(r#"{"a":{"b":2},"c":null}"#.into())]).unwrap(),
            Value::Integer(3));
    }
}

#[cfg(test)]
mod json_depth_wide_object_tests {
    use super::*;

    #[test]
    fn computes_large_wide_objects_without_heap_worklists() {
        let flat = serde_json::json!({
            "rows": (0..20_000).collect::<Vec<_>>(),
            "meta": {"a": [null, {"b": true}]},
        }).to_string();
        assert_eq!(json_depth(&[Value::Text(flat)]).unwrap(), Value::Integer(5));
    }

    #[test]
    fn empty_and_nested_containers_preserve_depth_semantics() {
        for (json, expected) in [
            ("{}", 1), ("[]", 1), ("null", 1),
            ("[1,2]", 2), ("{\"a\":{\"b\":[{}]}}", 4),
            ("[[], [1, [2]]]", 4),
        ] {
            assert_eq!(json_depth(&[Value::Text(json.into())]).unwrap(),
                Value::Integer(expected));
        }
        assert_eq!(json_depth(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod streaming_root_json_type_tests {
    use super::*;

    #[test]
    fn classifies_large_root_arrays_and_objects_without_materialization() {
        let array = format!("[{}]", vec![r#"{"data":[1,2,"🙂"]}"#; 10_000].join(","));
        assert_eq!(evaluate(&[Value::Text(array)]).unwrap(),
            Value::Text("ARRAY".into()));
        let object = format!(r#"{{"items":[{}],"meta":{{"key":"é"}}}}"#,
            vec!["null"; 20_000].join(","));
        assert_eq!(evaluate(&[Value::Text(object)]).unwrap(),
            Value::Text("OBJECT".into()));
        for (text, expected) in [
            ("null", "NULL"), ("false", "BOOLEAN"), ("42", "NUMBER"),
            ("-1.5e3", "NUMBER"), (r#""é🙂""#, "STRING"),
            ("[]", "ARRAY"), ("{}", "OBJECT"),
        ] {
            assert_eq!(evaluate(&[Value::Text(text.into())]).unwrap(),
                Value::Text(expected.into()));
        }
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn rejects_invalid_json_and_keeps_nested_path_behavior() {
        for invalid in ["[1,INVALID]", "{\"a\":1} trailing", "{bad}", ""] {
            assert!(evaluate(&[Value::Text(invalid.into())]).is_err(),
                "{invalid}");
        }
        assert_eq!(evaluate(&[Value::Text("[1,\"x\"]".into()),
            Value::Text("$[1]".into())]).unwrap(), Value::Text("STRING".into()));
        assert_eq!(evaluate(&[Value::Text("[1,\"x\"]".into()),
            Value::Text("$[99]".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod streaming_json_root_member_type_tests {
    use super::*;

    #[test]
    fn direct_member_types_match_full_tree_including_last_duplicate() {
        for (document, path) in [
            (r#"{"target":null,"wide":[{},[],true,100]}"#, "$.target"),
            (r#"{"target":[1,null,{"x":2}]}"#, "$.target"),
            (r#"{"target":{"nested":[1,2]}}"#, "$.target"),
            (r#"{"target":false}"#, "$.target"),
            (r#"{"target":1,"target":"last"}"#, "$.target"),
            (r#"{"t\u0061rget":0}"#, "$.target"),
            (r#"{"other":42}"#, "$.target"),
            (r#"{"nested":{"target":5}}"#, "$.nested.target"),
        ] {
            let result = evaluate(&[
                Value::Text(document.into()), Value::Text(path.into()),
            ]).unwrap();
            let doc = json_path::parse(document).unwrap();
            let expected = match json_path::lookup(Some(&doc), path).unwrap() {
                None => Value::Null,
                Some(serde_json::Value::Null) => Value::Text("NULL".into()),
                Some(serde_json::Value::String(_)) => Value::Text("STRING".into()),
                Some(serde_json::Value::Array(_)) => Value::Text("ARRAY".into()),
                Some(serde_json::Value::Object(_)) => Value::Text("OBJECT".into()),
                Some(serde_json::Value::Number(_)) => Value::Text("NUMBER".into()),
                Some(serde_json::Value::Bool(_)) => Value::Text("BOOLEAN".into()),
            };
            assert_eq!(result, expected, "{document} {path}");
        }
    }

    #[test]
    fn selected_field_never_skips_corruption_in_unselected_values() {
        for invalid in [
            r#"{"target":1,"broken":[1,,2]}"#,
            r#"{"target":null} trailing"#,
            r#"{"broken":[1,],"target":null}"#,
        ] {
            assert!(evaluate(&[
                Value::Text(invalid.into()), Value::Text("$.target".into()),
            ]).is_err(), "{invalid}");
        }
    }
}

#[cfg(test)]
mod streaming_json_depth_differential_tests {
    use super::*;

    fn reference(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Array(items) =>
                1 + items.iter().map(reference).max().unwrap_or(0),
            serde_json::Value::Object(items) =>
                1 + items.values().map(reference).max().unwrap_or(0),
            _ => 1,
        }
    }

    #[test]
    fn streams_large_arrays_and_nested_objects_without_a_value_tree() {
        let wide = (0..10_000).map(|n|
            format!(r#"{{"id":{n},"nested":[null,{{"value":"🙂"}}]}}"#)
        ).collect::<Vec<_>>().join(",");
        for document in [
            format!("[{wide}]"),
            r#"{"a":1,"b":[null,{"x":[true]}]}"#.to_string(),
            r#"{"x":{"deep":{"ignored":5}},"x":0}"#.to_string(),
            r#"{"x":0,"x":{"deep":{"kept":5}}}"#.to_string(),
            r#"{"\u0061":[1],"a":null}"#.to_string(),
            "null".to_string(), "true".to_string(), r#""é🙂""#.to_string(),
            "[]".to_string(), "{}".to_string(),
        ] {
            let parsed = serde_json::from_str::<serde_json::Value>(&document).unwrap();
            assert_eq!(json_depth(&[Value::Text(document)]).unwrap(),
                Value::Integer(reference(&parsed) as i64));
        }
    }

    #[test]
    fn malformed_documents_and_byte_limits_are_preserved() {
        for invalid in ["", "[1,2", "[1] trailing", r#"{"x":[}"#, "1e999"] {
            assert!(json_depth(&[Value::Text(invalid.into())]).is_err(),
                "invalid={invalid:?}");
        }
        assert!(matches!(json_depth(&[
            Value::Text(" ".repeat(16 * 1024 * 1024 + 1)),
        ]), Err(Error::Execution(_))));
        assert_eq!(json_depth(&[Value::Null]).unwrap(), Value::Null);
    }
}
