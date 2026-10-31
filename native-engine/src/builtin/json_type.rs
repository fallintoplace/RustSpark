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
    if !matches!(document, serde_json::Value::Array(_) | serde_json::Value::Object(_)) {
        return Ok(Value::Integer(1));
    }
    let mut nodes = vec![(&document, 1_i64)];
    let mut maximum = 0_i64;
    while let Some((node, depth)) = nodes.pop() {
        maximum = maximum.max(depth);
        match node {
            serde_json::Value::Array(items) => {
                for child in items {
                    maximum = maximum.max(depth + 1);
                    if matches!(child, serde_json::Value::Array(_) | serde_json::Value::Object(_)) {
                        nodes.push((child, depth + 1));
                    }
                }
            }
            serde_json::Value::Object(fields) => {
                for child in fields.values() {
                    maximum = maximum.max(depth + 1);
                    if matches!(child, serde_json::Value::Array(_) | serde_json::Value::Object(_)) {
                        nodes.push((child, depth + 1));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Value::Integer(maximum))
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
mod json_depth_scalar_fast_path_tests {
    use super::*;
    #[test]
    fn scalar_roots_have_depth_one_and_objects_still_traverse() {
        for input in ["null", "true", "5", r#""café""#] {
            assert_eq!(json_depth(&[Value::Text(input.into())]).unwrap(), Value::Integer(1));
        }
        assert_eq!(json_depth(&[Value::Text("[[1]]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(json_depth(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod wide_json_depth_tests {
    use super::*;

    #[test]
    fn flat_wide_arrays_and_nested_containers_keep_correct_depth() {
        let wide = format!("[{}]", vec!["null"; 20_000].join(","));
        assert_eq!(json_depth(&[Value::Text(wide)]).unwrap(), Value::Integer(2));
        assert_eq!(json_depth(&[Value::Text("[[1],true]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(json_depth(&[Value::Text(r#"{"a":{"b":2},"c":null}"#.into())]).unwrap(),
            Value::Integer(3));
    }
}
