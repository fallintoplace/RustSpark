use super::{Function, json_path};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

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
        Some(serde_json::Value::Array(items)) => Ok(Value::Integer(i64::try_from(items.len())
            .map_err(|_| Error::Execution("JSON array length overflow".into()))?)),
        _ => Ok(Value::Null),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "json_array_length", min_args: 1, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_json_elements_and_distinguishes_missing_from_empty() {
        assert_eq!(evaluate(&[Value::Text("[1,null,3]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("[]".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("{}".into()), Value::Text("$.missing".into())])
            .unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("{}".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("42".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("[1,2".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
