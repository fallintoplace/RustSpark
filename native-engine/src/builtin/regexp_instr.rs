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
    // Both patterns match at the first Unicode character boundary.
    if pattern.is_empty() || pattern == ".*" {
        return Ok(Value::Integer(1));
    }
    // A start-anchored identifier literal matches only at SQL position 1.
    if let Some(prefix) = pattern.strip_prefix('^') {
        if prefix.len() <= 4095 && prefix.bytes().all(|byte|
            byte.is_ascii_alphanumeric() || byte == b'_') {
            return Ok(Value::Integer(if text.starts_with(prefix) { 1 } else { 0 }));
        }
    }
    if pattern == "." {
        // Unlike a byte position, the SQL result counts Unicode scalars.
        let position = text.chars().position(|ch| ch != '\n')
            .map_or(0, |index| index + 1);
        return i64::try_from(position).map(Value::Integer)
            .map_err(|_| Error::Execution("REGEXP_INSTR position overflow".into()));
    }
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

#[cfg(test)]
mod dot_pattern_instr_tests {
    use super::*;

    #[test]
    fn dot_positions_match_regex_unicode_offsets() {
        let re = regex_support::compile(".").unwrap();
        for text in ["", "\n", "\n\n", "\n🙂z", "é\n🙂", "\r\n", "東京"] {
            let expected = re.find(text)
                .map_or(0, |matched| text[..matched.start()].chars().count() + 1);
            assert_eq!(
                evaluate(&[Value::Text(text.into()), Value::Text(".".into())]).unwrap(),
                Value::Integer(expected as i64)
            );
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod anchored_literal_instr_fast_path_tests {
    use super::*;

    #[test]
    fn anchored_positions_match_regex_on_unicode_haystacks() {
        for text in ["", "foo", "foobar", "xfoo", "\nfoo", "éfoo", "foo\nfoo"] {
            for pattern in ["^", "^foo", "^foo_bar", "^1", "^FOO", "^f.o", "(?m)^foo"] {
                let expected = regex_support::compile(pattern).unwrap().find(text)
                    .map_or(0, |matched| text[..matched.start()].chars().count() + 1);
                let actual = evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap();
                assert_eq!(actual, Value::Integer(expected as i64),
                    "pattern={pattern:?} text={text:?}");
            }
        }
    }
}

#[cfg(test)]
mod zero_width_regexp_instr_regressions {
    use super::*;

    #[test]
    fn empty_and_dot_star_patterns_match_first_unicode_boundary() {
        for text in ["", "\n", "é🙂", "\nabc", "東京\n🙂"] {
            for pattern in ["", ".*"] {
                let regex = regex_support::compile(pattern).unwrap();
                let expected = regex.find(text).map_or(0,
                    |matched| text[..matched.start()].chars().count() + 1);
                assert_eq!(evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap(), Value::Integer(expected as i64));
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".*".into())]).unwrap(), Value::Null);
    }
}
