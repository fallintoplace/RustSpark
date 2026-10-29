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


fn json_depth_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn json_depth(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let document = json_path::parse(text)?;
    // serde_json bounds nesting depth when parsing. Recurse by nesting level
    // instead of allocating an entry for every sibling in a wide array.
    fn depth(value: &serde_json::Value) -> i64 {
        let children = match value {
            serde_json::Value::Array(items) => items.iter().map(depth).max(),
            serde_json::Value::Object(fields) => fields.values().map(depth).max(),
            _ => None,
        };
        children.map_or(1, |maximum| maximum + 1)
    }
    Ok(Value::Integer(depth(&document)))
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
mod json_depth_wide_collections_tests {
    use super::*;

    #[test]
    fn wide_collections_have_small_depth_and_nested_values_count() {
        let wide = format!("[{}]", vec!["0"; 10_000].join(","));
        assert_eq!(json_depth(&[Value::Text(wide)]).unwrap(), Value::Integer(2));
        let nested = format!("{}0{}", "[".repeat(80), "]".repeat(80));
        assert_eq!(json_depth(&[Value::Text(nested)]).unwrap(), Value::Integer(81));
        assert_eq!(json_depth(&[Value::Text("{}".into())]).unwrap(), Value::Integer(1));
    }
}
