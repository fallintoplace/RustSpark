use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::{Deserialize, de::{IgnoredAny, SeqAccess, Visitor}};
use std::fmt;

struct RootArrayLength(usize);

impl<'de> Deserialize<'de> for RootArrayLength {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D)
        -> std::result::Result<Self, D::Error>
    {
        struct Counter;
        impl<'de> Visitor<'de> for Counter {
            type Value = RootArrayLength;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON array")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A)
                -> std::result::Result<Self::Value, A::Error>
            {
                let mut count = 0usize;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    count = count.checked_add(1).ok_or_else(||
                        serde::de::Error::custom("JSON array length overflow"))?;
                }
                Ok(RootArrayLength(count))
            }
        }
        deserializer.deserialize_seq(Counter)
    }
}

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
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
    if path == "$" && text.trim_start().starts_with('[') {
        // Counting a root array only needs JSON syntax validation, not an
        // allocated serde_json::Value tree for every element.
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let count = RootArrayLength::deserialize(&mut deserializer)
            .and_then(|result| deserializer.end().map(|()| result.0));
        return match count {
            Ok(count) => Ok(Value::Integer(i64::try_from(count)
                .map_err(|_| Error::Execution("JSON array length overflow".into()))?)),
            Err(_) => Ok(Value::Null),
        };
    }
    let doc = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(doc) => doc,
        Err(_) => return Ok(Value::Null),
    };
    match json_path::lookup(Some(&doc), path)? {
        None | Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::Array(items)) => Ok(Value::Integer(i64::try_from(items.len())
            .map_err(|_| Error::Execution("JSON array length overflow".into()))?)),
        _ => Ok(Value::Null),
    }
}


fn json_size_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn json_size(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let doc = json_path::parse(text)?;
    let count = match doc {
        serde_json::Value::Array(items) => items.len(),
        serde_json::Value::Object(entries) => entries.len(),
        serde_json::Value::Null => 0,
        _ => 1,
    };
    Ok(Value::Integer(i64::try_from(count)
        .map_err(|_| Error::Execution("JSON_SIZE overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_size", min_args: 1, max_args: 1, result_type: json_size_type, call: json_size },Function {
    name: "json_array_length", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_json_elements_and_distinguishes_missing_from_empty() {
        assert_eq!(evaluate(&[Value::Text("[1,null,3]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("[]".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("{}".into()), Value::Text("$.missing".into())])
            .unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("{}".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("42".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("[1,2".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_size_extension_tests {
    use super::*;
    #[test]
    fn checks_data_shapes_and_nulls() {
        assert!(crate::builtin::supports("json_size"));
        assert_eq!(json_size(&[Value::Text(r#"{"a":1,"b":2}"#.into())]).unwrap(), Value::Integer(2));
        assert_eq!(json_size(&[Value::Text("[null,1,2]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(json_size(&[Value::Text("true".into())]).unwrap(), Value::Integer(1));
        assert_eq!(json_size(&[Value::Text("null".into())]).unwrap(), Value::Integer(0));
        assert_eq!(json_size(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod root_array_streaming_length_tests {
    use super::*;

    fn length(text: &str, path: Option<&str>) -> Result<Value> {
        let mut args = vec![Value::Text(text.into())];
        if let Some(path) = path { args.push(Value::Text(path.into())); }
        evaluate(&args)
    }

    #[test]
    fn counts_nested_values_without_allocating_an_element_tree() {
        let elements = (0..10_000).map(|index|
            format!(r#"{{"number":{index},"text":"🙂é","nested":[null,true,{{"x":3}}]}}"#)
        ).collect::<Vec<_>>().join(",");
        let input = format!(" \n[{elements}] \n");
        assert_eq!(length(&input, Some("$")).unwrap(), Value::Integer(10_000));
        assert_eq!(length(" [1, null, [2, 3], {\"x\":2}]", None).unwrap(),
            Value::Integer(4));
        assert_eq!(length("[]", None).unwrap(), Value::Integer(0));
    }

    #[test]
    fn validates_entire_document_and_keeps_nonroot_path_semantics() {
        assert_eq!(length("[1,2] true", None).unwrap(), Value::Null);
        assert_eq!(length("[1,2", None).unwrap(), Value::Null);
        assert_eq!(length("[1,,2]", None).unwrap(), Value::Null);
        assert_eq!(length("{}", None).unwrap(), Value::Null);
        assert_eq!(length(r#"{"nested":[1,2,3]}"#, Some("$.nested")).unwrap(),
            Value::Integer(3));
        assert_eq!(length("null", None).unwrap(), Value::Null);
    }
}
