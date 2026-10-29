use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
    let document = json_path::parse(text)?;
    match json_path::lookup(Some(&document), path)? {
        Some(value @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) =>
            Ok(Value::Text(value.to_string())),
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
    let left = json_path::parse(left)?;
    let right = json_path::parse(right)?;
    Ok(Value::Boolean(left == right))
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
