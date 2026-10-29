use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(value) = super::string(&args[1])? else { return Ok(Value::Null); };
        value
    } else { "$" };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("native JSON input exceeds 16 MiB".into()));
    }
    let doc = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(doc) => doc,
        Err(_) => return Ok(Value::Null),
    };
    match json_path::lookup(Some(&doc), path)? {
        None | Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::Object(map)) => {
            let keys = map.keys().collect::<Vec<_>>();
            let encoded = serde_json::to_string(&keys)
                .map_err(|error| Error::Execution(format!("JSON_OBJECT_KEYS serialization failed: {error}")))?;
            if encoded.len() > 16 * 1024 * 1024 {
                return Err(Error::Execution("JSON_OBJECT_KEYS output exceeds 16 MiB".into()));
            }
            Ok(Value::Text(encoded))
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
mod escaped_json_object_key_tests {
    use super::*;
    #[test]
    fn serializes_escaped_and_unicode_keys_without_losing_information() {
        let document = Value::Text(r#"{"line\\n":1,"東京":2}"#.into());
        let Value::Text(encoded) = evaluate(&[document]).unwrap() else {
            panic!("expected JSON key array");
        };
        let keys: Vec<String> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(keys, vec!["line\\n", "東京"]);
    }
}
