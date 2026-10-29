use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(value) = super::string(&args[1])? else { return Ok(Value::Null); };
        value
    } else { "$" };
    let doc = json_path::parse(text)?;
    match json_path::lookup(Some(&doc), path)? {
        None | Some(serde_json::Value::Null) => Ok(Value::Null),
        Some(serde_json::Value::Object(map)) => {
            let keys = map.keys().collect::<Vec<_>>();
            let encoded = serde_json::to_string(&keys)
                .map_err(|error| Error::Execution(format!("JSON_OBJECT_KEYS serialization failed: {error}")))?;
            Ok(Value::Text(encoded))
        }
        _ => Err(Error::Type("JSON_OBJECT_KEYS requires an object".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
        assert!(evaluate(&[Value::Text("[]".into())]).is_err());
        assert_eq!(evaluate(&[Value::Text(r#"{"x":1,"y":2,"z":3}"#.into())]).unwrap(),
            Value::Text(r#"["x","y","z"]"#.into()));
    }
}
