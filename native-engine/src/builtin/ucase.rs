use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("UCASE input exceeds 16 MiB".into()));
    }
    let converted = if text.is_ascii() {
        text.to_ascii_uppercase()
    } else {
        text.to_uppercase()
    };
    if converted.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("UCASE output exceeds 16 MiB".into()));
    }
    Ok(Value::Text(converted))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "ucase", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("café".into())]).unwrap(), Value::Text("CAFÉ".into()));
        assert_eq!(evaluate(&[Value::Text("hello world 123".into())]).unwrap(),
            Value::Text("HELLO WORLD 123".into()));
        assert_eq!(evaluate(&[Value::Text("straße".into())]).unwrap(),
            Value::Text("STRASSE".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod bounded_unicode_ucase_tests {
    use super::*;
    #[test]
    fn preserves_uppercase_expansion_and_rejects_oversize_input() {
        assert_eq!(evaluate(&[Value::Text("straße".into())]).unwrap(),
            Value::Text("STRASSE".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("x".repeat(16 * 1024 * 1024 + 1))]).is_err());
    }
}
