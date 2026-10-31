use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null) };
    regex_support::check_input(text)?;
    if pattern.is_empty() { return Ok(Value::Text(String::new())); }
    if pattern == ".*" {
        // A greedy unanchored dot-star match starts at zero and stops at LF.
        return Ok(Value::Text(text.split('\n').next().unwrap_or("").to_owned()));
    }
    if pattern == "." {
        // The default regex dot matches one Unicode scalar other than LF.
        return Ok(text.chars().find(|&ch| ch != '\n')
            .map_or(Value::Null, |ch| Value::Text(ch.to_string())));
    }
    if pattern.len() <= 4096 && pattern.bytes().all(|byte|
        byte.is_ascii_alphanumeric() || byte == b'_') {
        return Ok(text.find(pattern)
            .map_or(Value::Null, |_| Value::Text(pattern.to_owned())));
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

#[cfg(test)]
mod literal_regex_substr_tests {
    use super::*;

    #[test]
    fn literal_fast_path_preserves_null_and_match_results() {
        for (text, pattern, expected) in [
            ("éfoo_bar", "foo", Value::Text("foo".into())),
            ("foofoo", "foo", Value::Text("foo".into())),
            ("no match", "xyz", Value::Null),
            ("", "foo", Value::Null),
            ("abc", "", Value::Text(String::new())),
        ] {
            let args = [Value::Text(text.into()), Value::Text(pattern.into())];
            assert_eq!(evaluate(&args).unwrap(), expected);
        }
    }
}

#[cfg(test)]
mod dot_pattern_substring_tests {
    use super::*;

    #[test]
    fn first_non_newline_scalar_matches_regex() {
        let re = regex_support::compile(".").unwrap();
        for text in ["", "\n", "\n\n", "\n🙂tail", "é\n🙂", "\r\nx", "東京"] {
            let expected = re.find(text)
                .map_or(Value::Null, |matched| Value::Text(matched.as_str().into()));
            assert_eq!(
                evaluate(&[Value::Text(text.into()), Value::Text(".".into())]).unwrap(),
                expected
            );
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod greedy_dot_star_substr_regressions {
    use super::*;

    #[test]
    fn extracts_the_same_first_match_as_the_regex_engine() {
        let regex = regex_support::compile(".*").unwrap();
        for text in ["", "\n", "\n\n", "abc\ndef", "é🙂\n東京", "\r\n", "東京"] {
            let expected = regex.find(text).map_or(Value::Null,
                |matched| Value::Text(matched.as_str().into()));
            assert_eq!(evaluate(&[
                Value::Text(text.into()), Value::Text(".*".into()),
            ]).unwrap(), expected);
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text(".*".into())]).unwrap(),
            Value::Null);
    }
}
