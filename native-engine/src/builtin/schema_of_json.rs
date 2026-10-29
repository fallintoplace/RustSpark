use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}

fn infer(value: &serde_json::Value, depth: usize) -> Result<String> {
    if depth > 64 {
        return Err(Error::Execution("SCHEMA_OF_JSON nesting exceeds 64 levels".into()));
    }
    match value {
        serde_json::Value::Null => Ok("VOID".into()),
        serde_json::Value::Bool(_) => Ok("BOOLEAN".into()),
        serde_json::Value::Number(n) if n.is_i64() || n.as_u64().is_some_and(|n| n <= i64::MAX as u64) =>
            Ok("BIGINT".into()),
        serde_json::Value::Number(_) => Ok("DOUBLE".into()),
        serde_json::Value::String(_) => Ok("STRING".into()),
        serde_json::Value::Array(values) => {
            let mut element = String::from("VOID");
            for value in values {
                let next = infer(value, depth + 1)?;
                if element == "VOID" {
                    element = next;
                } else if next == "VOID" || next == element {
                    continue;
                } else if (element == "BIGINT" && next == "DOUBLE")
                    || (element == "DOUBLE" && next == "BIGINT")
                {
                    element = "DOUBLE".into();
                } else {
                    element = "STRING".into();
                }
            }
            if element == "VOID" { element = "STRING".into(); }
            Ok(format!("ARRAY<{element}>"))
        }
        serde_json::Value::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            for (key, value) in map {
                let name = if key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                    && !key.is_empty() {
                    key.to_string()
                } else {
                    format!("`{}`", key.replace('`', "``"))
                };
                fields.push(format!("{name}: {}", infer(value, depth + 1)?));
            }
            Ok(format!("STRUCT<{}>", fields.join(", ")))
        }
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("SCHEMA_OF_JSON input exceeds native 16 MiB budget".into()));
    }
    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|error| Error::Execution(format!("SCHEMA_OF_JSON invalid input: {error}")))?;
    Ok(Value::Text(infer(&value, 0)?))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "schema_of_json", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_nested_schemas_and_promotes_array_numbers() {
        assert_eq!(evaluate(&[Value::Text(r#"{"a":1,"b":[1,2.5]}"#.into())]).unwrap(),
            Value::Text("STRUCT<a: BIGINT, b: ARRAY<DOUBLE>>".into()));
        assert_eq!(evaluate(&[Value::Text(r#"{"user name":{"x":true}}"#.into())]).unwrap(),
            Value::Text("STRUCT<`user name`: STRUCT<x: BOOLEAN>>".into()));
        assert_eq!(evaluate(&[Value::Text("[]".into())]).unwrap(),
            Value::Text("ARRAY<STRING>".into()));
        assert!(evaluate(&[Value::Text("{bad".into())]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
