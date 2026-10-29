use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let path = if args.len() == 2 {
        let Some(path) = super::string(&args[1])? else { return Ok(Value::Null); };
        path
    } else { "$" };
    let doc = json_path::parse(text)?;
    Ok(json_path::lookup(Some(&doc), path)?
        .map_or(Value::Null, |value| Value::Text(value.to_string())))
}


fn json_array_at_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(crate::Error::Type("JSON_ARRAY_AT expects JSON text and integer index".into()))
    }
}
fn json_array_at(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(index) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let doc = json_path::parse(text)?;
    match doc {
        serde_json::Value::Array(items) => Ok(usize::try_from(index).ok()
            .and_then(|offset| items.get(offset))
            .map_or(Value::Null, |item| Value::Text(item.to_string()))),
        _ => Err(crate::Error::Type("JSON_ARRAY_AT requires an array".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "json_array_at", min_args: 2, max_args: 2, result_type: json_array_at_type, call: json_array_at },Function {
    name: "json_extract", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_canonical_json_including_literal_null() {
        let doc = Value::Text(r#"{"n":4,"missing":null}"#.into());
        assert_eq!(evaluate(&[doc.clone(), Value::Text("$.n".into())]).unwrap(),
            Value::Text("4".into()));
        assert_eq!(evaluate(&[doc.clone(), Value::Text("$.missing".into())]).unwrap(),
            Value::Text("null".into()));
        assert_eq!(evaluate(&[doc, Value::Text("$.absent".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod json_array_at_extension_tests {
    use super::*;
    #[test]
    fn handles_json_values_and_missing_data() {
        assert!(crate::builtin::supports("json_array_at"));
        let arr = Value::Text(r#"[1,null,{"x":2}]"#.into());
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(0)]).unwrap(), Value::Text("1".into()));
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(1)]).unwrap(), Value::Text("null".into()));
        assert_eq!(json_array_at(&[arr.clone(), Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(json_array_at(&[arr, Value::Integer(7)]).unwrap(), Value::Null);
        assert_eq!(json_array_at(&[Value::Null, Value::Integer(0)]).unwrap(), Value::Null);
    }
}
