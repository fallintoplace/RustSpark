use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::de::{DeserializeSeed, IgnoredAny, SeqAccess, Visitor};
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

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
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
