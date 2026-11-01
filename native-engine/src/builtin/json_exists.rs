use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::fmt;

// Deserialize one root array element at a time instead of holding the entire
// collection in memory. After a match, still parse the remaining elements to
// reject malformed input rather than accepting a valid prefix.
struct ContainsSeed<'a>(&'a serde_json::Value);

impl<'de> DeserializeSeed<'de> for ContainsSeed<'_> {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D)
        -> std::result::Result<bool, D::Error>
    {
        struct ArrayVisitor<'a>(&'a serde_json::Value);

        impl<'de> Visitor<'de> for ArrayVisitor<'_> {
            type Value = bool;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON array")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<bool, A::Error>
            {
                while let Some(value) = seq.next_element::<serde_json::Value>()? {
                    if value == *self.0 {
                        while seq.next_element::<IgnoredAny>()?.is_some() {}
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
        deserializer.deserialize_seq(ArrayVisitor(self.0))
    }
}

// A direct $.name lookup only needs the root keys. Scan and validate all
// values, including the tail after a match, without building a JSON tree.
struct RootMemberExists<'a>(&'a str);

impl<'de> DeserializeSeed<'de> for RootMemberExists<'_> {
    type Value = bool;

    fn deserialize<D: serde::Deserializer<'de>>(
        self, deserializer: D,
    ) -> std::result::Result<bool, D::Error> {
        struct RootVisitor<'a>(&'a str);
        impl<'de> Visitor<'de> for RootVisitor<'_> {
            type Value = bool;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(
                self, mut map: A,
            ) -> std::result::Result<bool, A::Error> {
                let mut found = false;
                while let Some(name) = map.next_key::<String>()? {
                    found |= name == self.0;
                    map.next_value::<IgnoredAny>()?;
                }
                Ok(found)
            }
        }
        deserializer.deserialize_map(RootVisitor(self.0))
    }
}

fn simple_root_member(path: &str) -> Option<&str> {
    let member = path.strip_prefix("$.")?;
    (path.len() <= 4096 && !member.is_empty() &&
        member.bytes().all(|ch| ch.is_ascii_alphanumeric() || ch == b'_'))
        .then_some(member)
}

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    if path == "$" {
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        // Any well-formed root JSON value exists, including literal null.
        // Validate its entire syntax without allocating its nested tree.
        serde_json::from_str::<IgnoredAny>(text)
            .map_err(|error| Error::Execution(format!("invalid native JSON input: {error}")))?;
        return Ok(Value::Boolean(true));
    }
    if let Some(member) = simple_root_member(path)
        && text.trim_start().starts_with('{')
    {
        if text.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let exists = RootMemberExists(member).deserialize(&mut deserializer)
            .and_then(|exists| deserializer.end().map(|()| exists))
            .map_err(|error| Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return Ok(Value::Boolean(exists));
    }
    let doc = json_path::parse(text)?;
    Ok(Value::Boolean(json_path::lookup(Some(&doc), path)?.is_some()))
}


fn json_array_contains_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}
fn json_array_contains(args: &[Value]) -> Result<Value> {
    let Some(document) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(needle) = super::string(&args[1])? else { return Ok(Value::Null); };
    if document.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    if document.trim_start().starts_with('[') {
        let needle = json_path::parse(needle)?;
        let mut deserializer = serde_json::Deserializer::from_str(document);
        let found = ContainsSeed(&needle).deserialize(&mut deserializer)
            .and_then(|found| deserializer.end().map(|()| found))
            .map_err(|error| Error::Execution(format!(
                "invalid native JSON input: {error}"
            )))?;
        return Ok(Value::Boolean(found));
    }
    let document = json_path::parse(document)?;
    let needle = json_path::parse(needle)?;
    match document {
        serde_json::Value::Array(values) => Ok(Value::Boolean(values.iter().any(|item| item == &needle))),
        _ => Err(Error::Type("JSON_ARRAY_CONTAINS requires an array".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_array_contains", min_args: 2, max_args: 2, result_type: json_array_contains_type, call: json_array_contains },Function {
    name: "json_exists", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distinguishes_json_null_members_from_missing_members() {
        let doc = Value::Text(r#"{"present":null}"#.into());
        assert_eq!(evaluate(&[doc.clone(), Value::Text("$.present".into())]).unwrap(),
            Value::Boolean(true));
        assert_eq!(evaluate(&[doc, Value::Text("$.missing".into())]).unwrap(),
            Value::Boolean(false));
    }
}

#[cfg(test)]
mod json_array_contains_extension_tests {
    use super::*;
    #[test]
    fn handles_json_values_and_missing_data() {
        assert!(crate::builtin::supports("json_array_contains"));
        let arr = Value::Text(r#"[1,{"x":2},null]"#.into());
        assert_eq!(json_array_contains(&[arr.clone(), Value::Text(r#"{"x":2}"#.into())]).unwrap(), Value::Boolean(true));
        assert_eq!(json_array_contains(&[arr.clone(), Value::Text("null".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(json_array_contains(&[arr, Value::Text("3".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(json_array_contains(&[Value::Null, Value::Text("1".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod streaming_json_array_membership_tests {
    use super::*;

    fn contains(document: &str, needle: &str) -> Result<Value> {
        json_array_contains(&[
            Value::Text(document.into()), Value::Text(needle.into())
        ])
    }

    #[test]
    fn large_arrays_and_nested_unicode_values_keep_json_equality_semantics() {
        let needle = r#"{"payload":"🙂é","nested":[null,true,7]}"#;
        let other = r#"{"payload":"other","nested":[1,2]}"#;
        let mut input = format!("[{}", other);
        for _ in 0..10_000 { input.push_str(",0"); }
        input.push(',');
        input.push_str(needle);
        input.push(']');
        assert_eq!(contains(&input, needle).unwrap(), Value::Boolean(true));
        assert_eq!(contains(&input, "null").unwrap(), Value::Boolean(false));
        assert_eq!(contains("[]", "null").unwrap(), Value::Boolean(false));
        assert_eq!(contains("[null,[1,2],true]", "null").unwrap(), Value::Boolean(true));
        assert_eq!(contains("[null,[1,2],true]", "[1,2]").unwrap(), Value::Boolean(true));
    }

    #[test]
    fn a_match_cannot_hide_malformed_remainder_or_trailing_tokens() {
        assert!(contains("[1,INVALID]", "1").is_err());
        assert!(contains("[1] trailing", "1").is_err());
        assert!(contains("[1", "1").is_err());
        assert!(contains("{}", "1").unwrap_err().to_string().contains("requires an array"));
        assert!(contains("[]", "{bad json}").is_err());
    }
}

#[cfg(test)]
mod streaming_root_json_exists_tests {
    use super::*;

    #[test]
    fn root_json_exists_handles_large_documents_and_json_null() {
        let large = format!(r#"{{"nested":[{}],"label":"é🙂"}}"#,
            vec![r#"{"values":[1,2,3]}"#; 10_000].join(","));
        for input in ["null", "true", "0", r#""é🙂""#, "[]", "{}"] {
            assert_eq!(evaluate(&[Value::Text(input.into()),
                Value::Text("$".into())]).unwrap(), Value::Boolean(true));
        }
        assert_eq!(evaluate(&[Value::Text(large),
            Value::Text("$".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("{}".into()),
            Value::Text("$.missing".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Null, Value::Text("$".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("{}".into()), Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn root_fast_path_rejects_invalid_json_and_trailing_tokens() {
        for input in ["", "[1,2", "[1,INVALID]", "{\"a\":1} false"] {
            assert!(evaluate(&[Value::Text(input.into()),
                Value::Text("$".into())]).is_err(), "{input}");
        }
        assert!(matches!(evaluate(&[Value::Text("a".repeat(16 * 1024 * 1024 + 1)),
            Value::Text("$".into())]), Err(Error::Execution(_))));
    }
}

#[cfg(test)]
mod streaming_direct_json_member_exists_tests {
    use super::*;

    #[test]
    fn direct_root_lookup_matches_dom_with_duplicate_and_escaped_keys() {
        for (doc, path) in [
            (r#"{"target":null,"skip":{"wide":[1,2,3]}}"#, "$.target"),
            (r#"{"skip":[{},1,2],"target":false}"#, "$.target"),
            (r#"{"target":1,"target":null}"#, "$.target"),
            (r#"{"t\\u0061rget":true}"#, "$.target"),
            (r#"{"missing":1}"#, "$.target"),
            (r#"{"target":{"nested":[null,1]}}"#, "$.target"),
            (r#"{"a":{"nested":null}}"#, "$.a.nested"),
        ] {
            let expected = json_path::lookup(Some(&json_path::parse(doc).unwrap()), path)
                .unwrap().is_some();
            let result = evaluate(&[
                Value::Text(doc.into()), Value::Text(path.into()),
            ]).unwrap();
            assert_eq!(result, Value::Boolean(expected), "{doc} {path}");
        }
    }

    #[test]
    fn validates_document_tail_even_after_a_member_was_found() {
        for invalid in [r#"{"target":true,"tail":[1,,2]}"#,
                        r#"{"target":1} garbage"#,
                        r#"{"target":1,"tail":"oops}"#] {
            assert!(evaluate(&[
                Value::Text(invalid.into()), Value::Text("$.target".into()),
            ]).is_err(), "{invalid}");
        }
        assert_eq!(evaluate(&[
            Value::Text("null".into()), Value::Text("$.target".into()),
        ]).unwrap(), Value::Boolean(false));
    }
}
