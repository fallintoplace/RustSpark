use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null) };
    regex_support::check_input(text)?;
    if !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        let Some(index) = text.find(pattern) else { return Ok(Value::Integer(0)); };
        let prefix = &text[..index];
        let count = super::char_length::character_count(prefix);
        return i64::try_from(count + 1).map(Value::Integer)
            .map_err(|_| Error::Execution("REGEXP_INSTR position overflow".into()));
    }
    let pattern = regex_support::compile(pattern)?;
    let Some(matched) = pattern.find(text) else { return Ok(Value::Integer(0)) };
    let prefix = &text[..matched.start()];
    let offset = super::char_length::character_count(prefix);
    let position = offset.checked_add(1)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| Error::Execution("REGEXP_INSTR position overflow".into()))?;
    Ok(Value::Integer(position))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_instr", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn returns_unicode_character_offset_or_zero() {
        assert_eq!(evaluate(&[Value::Text("café-34".into()), Value::Text("[0-9]+".into())]).unwrap(), Value::Integer(6));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("[0-9]+".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("abc".into()), Value::Text("(".into())]).is_err());
    }
}

#[cfg(test)]
mod regexp_ascii_position_perf_tests {
    use super::*;
    #[test]
    fn respects_ascii_and_unicode_character_positions() {
        assert_eq!(evaluate(&[Value::Text("abc123".into()), Value::Text("123".into())]).unwrap(), Value::Integer(4));
        assert_eq!(evaluate(&[Value::Text("éabc123".into()), Value::Text("123".into())]).unwrap(), Value::Integer(5));
    }
}

#[cfg(test)]
mod literal_regexp_instr_fast_path_tests {
    use super::*;
    #[test]
    fn counts_unicode_positions_for_literal_matches() {
        assert_eq!(evaluate(&[Value::Text("é🙂hello_123".into()),
            Value::Text("hello_123".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("hello".into()),
            Value::Text("nomatch".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("a123".into()),
            Value::Text("[0-9]+".into())]).unwrap(), Value::Integer(2));
        assert!(evaluate(&[Value::Text("hello".into()), Value::Text("(".into())]).is_err());
    }
}

#[cfg(test)]
mod regexp_instr_unicode_offset_tests {
    use super::*;

    #[test]
    fn literal_and_regex_patterns_use_character_positions() {
        for pattern in ["foo", "f.o"] {
            let args = [Value::Text("🙂éfoo".into()), Value::Text(pattern.into())];
            assert_eq!(evaluate(&args).unwrap(), Value::Integer(3));
        }
    }
}
