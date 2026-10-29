use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
        path
    } else { "$" };
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

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
