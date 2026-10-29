use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Text(if text.is_ascii() {
        text.to_ascii_lowercase()
    } else {
        text.to_lowercase()
    }))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "lcase", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("ÉCOLE".into())]).unwrap(), Value::Text("école".into()));
        assert_eq!(evaluate(&[Value::Text("HELLO WORLD 123".into())]).unwrap(),
            Value::Text("hello world 123".into()));
        assert_eq!(evaluate(&[Value::Text("İstanbul".into())]).unwrap(),
            Value::Text("i\u{307}stanbul".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}
