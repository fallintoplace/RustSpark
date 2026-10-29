use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result};

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
    let document = json_path::parse(document)?;
    let needle = json_path::parse(needle)?;
    match document {
        serde_json::Value::Array(values) => Ok(Value::Boolean(values.iter().any(|item| item == &needle))),
        _ => Err(crate::Error::Type("JSON_ARRAY_CONTAINS requires an array".into())),
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
