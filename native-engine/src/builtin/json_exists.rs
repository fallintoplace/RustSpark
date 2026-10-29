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

pub(super) const FUNCTIONS: &[Function] = &[Function {
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
