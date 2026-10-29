use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null) };
    regex_support::check_input(text)?;
    if !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        return Ok(text.find(pattern).map_or(Value::Null, |_| Value::Text(pattern.to_owned())));
    }
    let pattern = regex_support::compile(pattern)?;
    Ok(pattern.find(text).map_or(Value::Null, |m| Value::Text(m.as_str().to_string())))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_substr", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extracts_first_regex_match_without_byte_slicing_errors() {
        assert_eq!(evaluate(&[Value::Text("café-34".into()), Value::Text("[0-9]+".into())]).unwrap(), Value::Text("34".into()));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("[0-9]+".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("abc".into()), Value::Text("(".into())]).is_err());
    }
}

#[cfg(test)]
mod literal_regexp_substr_fast_path_tests {
    use super::*;
    #[test]
    fn preserves_literal_and_regex_extracts() {
        assert_eq!(evaluate(&[Value::Text("éabc_123🙂".into()),
            Value::Text("abc_123".into())]).unwrap(), Value::Text("abc_123".into()));
        assert_eq!(evaluate(&[Value::Text("abc".into()),
            Value::Text("xyz".into())]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Text("abc123".into()),
            Value::Text("[0-9]+".into())]).unwrap(), Value::Text("123".into()));
        assert!(evaluate(&[Value::Text("abc".into()), Value::Text("(".into())]).is_err());
    }
}
