use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(separator) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut text_bytes = 0usize;
    let mut non_null = 0usize;
    for arg in &args[1..] {
        if let Some(text) = super::string(arg)? {
            text_bytes = text_bytes.checked_add(text.len())
                .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
            non_null += 1;
        }
    }
    let separators = separator.len().checked_mul(non_null.saturating_sub(1))
        .ok_or_else(|| Error::Execution("CONCAT_WS separator overflow".into()))?;
    let total = text_bytes.checked_add(separators)
        .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
    if total > 16 * 1024 * 1024 {
        return Err(Error::Execution("CONCAT_WS output exceeds 16 MiB".into()));
    }
    let mut output = String::with_capacity(total);
    let mut first = true;
    for arg in &args[1..] {
        if let Some(text) = super::string(arg)? {
            if !first { output.push_str(separator); }
            first = false;
            output.push_str(text);
        }
    }
    Ok(Value::Text(output))
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

#[cfg(test)]
mod concat_ws_buffer_perf_tests {
    use super::*;
    #[test]
    fn preserves_empty_fields_separators_and_unicode() {
        assert_eq!(evaluate(&[Value::Text("-".into()), Value::Text("".into()),
            Value::Text("é".into()), Value::Null, Value::Text("".into())]).unwrap(),
            Value::Text("-é-".into()));
        assert_eq!(evaluate(&[Value::Text(",".into()), Value::Null, Value::Null]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Text("".into()), Value::Text("a".into()),
            Value::Text("b".into())]).unwrap(), Value::Text("ab".into()));
    }
}
