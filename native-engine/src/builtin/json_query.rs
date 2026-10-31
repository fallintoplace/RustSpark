use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};
use serde::de::IgnoredAny;

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
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
        if left.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
        }
        // Identical JSON text is structurally equal, but still needs full
        // syntax validation. Skip allocating a value tree in this case.
        serde_json::from_str::<IgnoredAny>(left)
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
    fn large_identical_json_is_validated_without_deserializing_values() {
        let text = format!(r#"{{"rows":[{}],"meta":{{"name":"é🙂"}}}}"#,
            vec![r#"{"nested":[true,false,null,42]}"#; 10_000].join(","));
        assert_eq!(json_equal(&[Value::Text(text.clone()),
            Value::Text(text)]).unwrap(), Value::Boolean(true));
        assert_eq!(json_equal(&[Value::Text("[1,2]".into()),
            Value::Text("[2,1]".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(json_equal(&[Value::Text(r#"{"a":1,"b":2}"#.into()),
            Value::Text(r#"{"b":2,"a":1}"#.into())]).unwrap(),
            Value::Boolean(true));
        assert_eq!(json_equal(&[Value::Null,
            Value::Text("null".into())]).unwrap(), Value::Null);
    }

    #[test]
    fn identical_invalid_input_still_fails_and_keeps_byte_limit() {
        for input in ["", "{broken", "[1,INVALID]", "[1] trailing"] {
            assert!(json_equal(&[Value::Text(input.into()),
                Value::Text(input.into())]).is_err(), "{input}");
        }
        let oversized = Value::Text(" ".repeat(16 * 1024 * 1024 + 1));
        assert!(matches!(json_equal(&[oversized.clone(), oversized]),
            Err(Error::Execution(_))));
    }
}
