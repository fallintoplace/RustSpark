use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(separator) = super::string(&args[0])? else { return Ok(Value::Null); };
    let mut total = 0usize;
    let mut pieces = 0usize;
    for arg in &args[1..] {
        if let Some(text) = super::string(arg)? {
            total = total.checked_add(text.len())
                .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
            pieces += 1;
        }
    }
    let separators = separator.len().checked_mul(pieces.saturating_sub(1))
        .ok_or_else(|| Error::Execution("CONCAT_WS separator overflow".into()))?;
    let length = total.checked_add(separators)
        .ok_or_else(|| Error::Execution("CONCAT_WS length overflow".into()))?;
    if length > 16 * 1024 * 1024 {
        return Err(Error::Execution("CONCAT_WS output exceeds 16 MiB".into()));
    }

    // The first pass validates the bound; the second writes once into the result.
    let mut output = String::with_capacity(length);
    let mut first = true;
    for arg in &args[1..] {
        if let Value::Text(text) = arg {
            if !first { output.push_str(separator); }
            output.push_str(text);
            first = false;
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "concat_ws", min_args: 1, max_args: 128,
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
        assert_eq!(evaluate(&[
            Value::Text("🙂".into()), Value::Null, Value::Text("a".into()),
            Value::Text("".into()), Value::Text("é".into()),
        ]).unwrap(), Value::Text("a🙂🙂é".into()));
        assert!(evaluate(&[
            Value::Text("-".into()), Value::Text("x".repeat(16 * 1024 * 1024)),
            Value::Text("z".into()),
        ]).is_err());
    }
}

#[cfg(test)]
mod separator_only_concat_ws_tests {
    use super::*;
    #[test]
    fn returns_empty_string_for_only_a_separator() {
        assert_eq!(evaluate(&[Value::Text(",".into())]).unwrap(), Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(output(&[DataType::Text]).unwrap(), DataType::Text);
    }
}

#[cfg(test)]
mod concat_ws_validated_output_tests {
    use super::*;
    #[test]
    fn preserves_null_skips_and_empty_element_separators() {
        let args = [
            Value::Text("🙂".into()), Value::Text("é".into()), Value::Null,
            Value::Text("".into()), Value::Text("東京".into()),
        ];
        assert_eq!(evaluate(&args).unwrap(), Value::Text("é🙂🙂東京".into()));
        assert_eq!(evaluate(&[Value::Text(",".into()), Value::Null]).unwrap(),
            Value::Text("".into()));
        assert!(evaluate(&[Value::Text(",".into()), Value::Integer(1)]).is_err());
    }
}
