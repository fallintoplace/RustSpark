//! Read the first Unicode scalar value from SQL text.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("UNICODE_CODEPOINT requires text input".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(text.chars().next().map_or(Value::Null, |character| Value::Integer(i64::from(u32::from(character)))))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "unicode_codepoint", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handles_unicode_ascii_empty_and_null_text() {
        assert_eq!(evaluate(&[Value::Text("ABC".into())]).unwrap(), Value::Integer(65));
        assert_eq!(evaluate(&[Value::Text("é".into())]).unwrap(), Value::Integer(233));
        assert_eq!(evaluate(&[Value::Text("🙂".into())]).unwrap(), Value::Integer(0x1F642));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
        assert!(crate::builtin::supports("UNICODE_CODEPOINT"));
    }
}
