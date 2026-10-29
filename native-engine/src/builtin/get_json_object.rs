use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("GET_JSON_OBJECT input exceeds native 16 MiB budget".into()));
    }
    let document: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return Ok(Value::Null),
    };
    let Some(value) = json_path::lookup(Some(&document), path)? else {
        return Ok(Value::Null);
    };
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(text) => Value::Text(text.clone()),
        other => Value::Text(other.to_string()),
    })
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "get_json_object", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_spark_json_scalars_and_objects() {
        let json = Value::Text(r#"{"s":"hello","n":7,"nested":{"ok":true},"array":[5,8]}"#.into());
        let get = |path: &str| evaluate(&[json.clone(), Value::Text(path.into())]).unwrap();
        assert_eq!(get("$.s"), Value::Text("hello".into()));
        assert_eq!(get("$.n"), Value::Text("7".into()));
        assert_eq!(get("$.nested"), Value::Text(r#"{"ok":true}"#.into()));
        assert_eq!(get("$.array[1]"), Value::Text("8".into()));
        assert_eq!(get("$.missing"), Value::Null);
        assert_eq!(evaluate(&[Value::Text("{bad".into()), Value::Text("$.x".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("$.x".into())]).unwrap(), Value::Null);
    }
}
