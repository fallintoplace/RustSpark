use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let length = text.len().checked_mul(8).and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Error::Execution("BIT_LENGTH overflow".into()))?;
    Ok(Value::Integer(length))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "bit_length", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("é🙂".into())]).unwrap(), Value::Integer(48));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}
