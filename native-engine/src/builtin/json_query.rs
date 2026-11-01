use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};

// Preserve JSON_QUERY's container-only semantics without retaining the
// rest of a wide root object. Duplicate keys use the last value.
struct RootMemberQuery<'a>(&'a str);

impl<'de> serde::de::DeserializeSeed<'de> for RootMemberQuery<'_> {
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
    let member = path.strip_prefix("$.")?;
    (path.len() <= 4096 && !member.is_empty() &&
        member.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_'))
        .then_some(member)
}

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    if let Some(member) = direct_root_member(path)
        && text.trim_start().starts_with('{')
    {
        use serde::de::DeserializeSeed;
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let selected = RootMemberQuery(member).deserialize(&mut deserializer)
            .and_then(|selected| deserializer.end().map(|()| selected))
            .map_err(|error| Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return match selected {
            Some(value @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) =>
                Ok(Value::Text(json_path::serialize_bounded(&value, 16 * 1024 * 1024)?)),
            _ => Ok(Value::Null),
        };
    }
    let document = json_path::parse(text)?;
    match json_path::lookup(Some(&document), path)? {
        Some(value @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) =>
            Ok(Value::Text(json_path::serialize_bounded(value, 16 * 1024 * 1024)?)),
        _ => Ok(Value::Null),
    }
}


fn json_equal_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}
fn json_equal(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
    if left == right {
        // Equal source text still needs syntax validation, but not a JSON tree.
        if left.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        serde_json::from_str::<serde::de::IgnoredAny>(left)
            .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))?;
        return Ok(Value::Boolean(true));
    }
    let left_doc = json_path::parse(left)?;
    let right_doc = json_path::parse(right)?;
    Ok(Value::Boolean(left_doc == right_doc))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_equal", min_args: 2, max_args: 2, result_type: json_equal_type, call: json_equal },Function {
    name: "json_query", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_json_arrays_and_objects_without_scalar_coercion() {
        let text = Value::Text(r#"{"a":[1,2],"b":{"n":4}}"#.into());
        assert_eq!(evaluate(&[text.clone(), Value::Text("$.a".into())]).unwrap(),
            Value::Text("[1,2]".into()));
        assert_eq!(evaluate(&[text.clone(), Value::Text("$.b".into())]).unwrap(),
            Value::Text(r#"{"n":4}"#.into()));
        assert_eq!(evaluate(&[text, Value::Text("$.b.n".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_equal_extension_tests {
    use super::*;
    #[test]
    fn handles_special_values_and_nulls() {
        assert!(crate::builtin::supports("json_equal"));
        assert_eq!(json_equal(&[Value::Text(r#"{"a":1,"b":2}"#.into()), Value::Text(r#"{ "b":2, "a":1 }"#.into())]).unwrap(), Value::Boolean(true));
        assert_eq!(json_equal(&[Value::Text("[1,2]".into()), Value::Text("[2,1]".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(json_equal(&[Value::Null, Value::Text("null".into())]).unwrap(), Value::Null);
        assert!(json_equal(&[Value::Text("{broken".into()), Value::Text("{}".into())]).is_err());
    }
}

#[cfg(test)]
mod identical_json_equality_tests {
    use super::*;

    #[test]
    fn compares_identical_json_without_ignoring_malformed_input() {
        let text = Value::Text(r#"{"nested":[1,2,null],"é":"🙂"}"#.into());
        assert_eq!(json_equal(&[text.clone(), text]).unwrap(), Value::Boolean(true));
        assert_eq!(json_equal(&[
            Value::Text(r#"{"a":1,"b":2}"#.into()),
            Value::Text(r#"{"b":2, "a":1}"#.into()),
        ]).unwrap(), Value::Boolean(true));
        assert!(json_equal(&[
            Value::Text("{invalid".into()), Value::Text("{invalid".into())
        ]).is_err());
        assert_eq!(json_equal(&[Value::Null, Value::Text("null".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod identical_json_stream_validation_tests {
    use super::*;

    #[test]
    fn identical_documents_validate_without_building_a_value_tree() {
        let document = format!("[{}]", vec![r#"{"nested":[1,null,{"é":"🙂"}]}"#; 12_000].join(","));
        let value = Value::Text(document);
        assert_eq!(json_equal(&[value.clone(), value]).unwrap(), Value::Boolean(true));

        for invalid in ["{broken", r#"{"ok":1} false"#, "[1,,2]", r#""unterminated"#] {
            assert!(matches!(json_equal(&[
                Value::Text(invalid.into()), Value::Text(invalid.into()),
            ]), Err(Error::Execution(_))), "{invalid}");
        }
        assert_eq!(json_equal(&[Value::Text(r#"{"a":1}"#.into()), Value::Text(r#"{ "a": 1 }"#.into())])
            .unwrap(), Value::Boolean(true));
    }
}

#[cfg(test)]
mod streaming_direct_json_query_tests {
    use super::*;

    #[test]
    fn selects_only_containers_with_last_duplicate_key() {
        for (document, expected) in [
            (r#"{"target":[1,{"x":true}],"unrelated":[1,2,3]}"#,
                Value::Text(r#"[1,{"x":true}]"#.into())),
            (r#"{"target":{"é":[true,null]}}"#,
                Value::Text(r#"{"é":[true,null]}"#.into())),
            (r#"{"target":7}"#, Value::Null),
            (r#"{"target":null}"#, Value::Null),
            (r#"{"target":[1],"target":42}"#, Value::Null),
            (r#"{"target":42,"target":[]}"#, Value::Text("[]".into())),
            (r#"{"t\u0061rget":{"x":1}}"#, Value::Text(r#"{"x":1}"#.into())),
            (r#"{"other":[1,2]}"#, Value::Null),
        ] {
            assert_eq!(evaluate(&[
                Value::Text(document.into()), Value::Text("$.target".into()),
            ]).unwrap(), expected, "{document}");
        }
    }

    #[test]
    fn checks_full_document_and_retains_nested_path_fallback() {
        assert!(evaluate(&[
            Value::Text(r#"{"target":[],"bad":[1,,2]}"#.into()),
            Value::Text("$.target".into()),
        ]).is_err());
        assert_eq!(evaluate(&[
            Value::Text(r#"{"nested":{"target":[1,null]}}"#.into()),
            Value::Text("$.nested.target".into()),
        ]).unwrap(), Value::Text("[1,null]".into()));
    }
}
