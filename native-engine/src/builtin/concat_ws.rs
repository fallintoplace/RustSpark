use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(separator) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut pieces = Vec::new();
    let mut total = 0usize;
    for arg in &args[1..] {
        if let Some(text) = super::string(arg)? {
            total = total.checked_add(text.len())
                .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
            pieces.push(text);
        }
    }
    let separators = separator.len().checked_mul(pieces.len().saturating_sub(1))
        .ok_or_else(|| Error::Execution("CONCAT_WS separator overflow".into()))?;
    if total.checked_add(separators).is_none_or(|length| length > 16 * 1024 * 1024) {
        return Err(Error::Execution("CONCAT_WS output exceeds 16 MiB".into()));
    }
    Ok(Value::Text(pieces.join(separator)))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "concat_ws", min_args: 2, max_args: 128,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn joins_nonnull_fields_with_separator() {
        assert_eq!(evaluate(&[
            Value::Text("-".into()), Value::Text("Berlin".into()),
            Value::Null, Value::Text("Germany".into()),
        ]).unwrap(), Value::Text("Berlin-Germany".into()));
        assert_eq!(evaluate(&[
            Value::Text("-".into()), Value::Null,
        ]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text("x".into()),
        ]).unwrap(), Value::Null);
    }
}
