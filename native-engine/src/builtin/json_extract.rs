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

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
