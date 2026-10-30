use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null) };
    regex_support::check_input(text)?;
    if pattern.is_empty() { return Ok(Value::Text(String::new())); }
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
mod empty_regex_substr_perf_tests {
    use super::*;
    #[test]
    fn empty_pattern_returns_first_zero_width_match() {
        assert_eq!(evaluate(&[Value::Text("é🙂".into()), Value::Text("".into())])
            .unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Text("".into()), Value::Text("".into())])
            .unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text("".into())]).unwrap(), Value::Null);
    }
}
