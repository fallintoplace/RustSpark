use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null) };
    regex_support::check_input(text)?;
    if pattern.is_empty() { return Ok(Value::Text(String::new())); }
    if pattern == "[0-9]" || pattern == "[0-9]+" {
        // Byte-wise digit searches preserve UTF-8 boundaries because all
        // matched bytes are ASCII. Only copy the resulting match.
        let Some(start) = text.bytes().position(|byte| byte.is_ascii_digit())
            else { return Ok(Value::Null); };
        let width = if pattern == "[0-9]" { 1 } else {
            text[start..].bytes().take_while(u8::is_ascii_digit).count()
        };
        return Ok(Value::Text(text[start..start + width].to_owned()));
    }
    if pattern == ".+" {
        // The first greedy nonempty dot run ends before the next LF.
        return Ok(text.split('\n').find(|part| !part.is_empty())
            .map_or(Value::Null, |part| Value::Text(part.to_owned())));
    }
    if pattern == ".*" {
        // A greedy unanchored dot-star match starts at zero and stops at LF.
        return Ok(Value::Text(text.split('\n').next().unwrap_or("").to_owned()));
    }
    if pattern == "." {
        // The default regex dot matches one Unicode scalar other than LF.
        return Ok(text.chars().find(|&ch| ch != '\n')
            .map_or(Value::Null, |ch| Value::Text(ch.to_string())));
    }
    if pattern.len() <= 4096 && pattern.chars().all(|ch|
        ch.is_alphanumeric() || ch == '_') {
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


#[cfg(test)]
mod greedy_dot_plus_substr_differential_tests {
    use super::*;

    #[test]
    fn preserves_first_greedy_run_with_unicode_and_empty_lines() {
        let regex = regex_support::compile(".+").unwrap();
        for input in ["", "\n", "\n\n", "abc\ndef", "\nabc\n", "é🙂\n東京", "\r\n", "\n\n🙂é\n"] {
            let expected = regex.find(input)
                .map_or(Value::Null, |found| Value::Text(found.as_str().to_owned()));
            assert_eq!(evaluate(&[
                Value::Text(input.into()), Value::Text(".+".into()),
            ]).unwrap(), expected, "{input:?}");
        }
        let long = format!("\n\n{}\ntrailing", "é🙂".repeat(100_000));
        assert_eq!(evaluate(&[
            Value::Text(long), Value::Text(".+".into()),
        ]).unwrap(), Value::Text("é🙂".repeat(100_000)));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text(".+".into()),
        ]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_digit_substring_regressions {
    use super::*;

    #[test]
    fn extracts_same_digit_runs_as_regex_on_unicode_inputs() {
        for text in [
            "", "00033more1", "abc-4.50", "é🙂99🦀01", "١٢٣",
            "a\n123\n4", "東京7界88", "𝟡x9",
        ] {
            for pattern in ["[0-9]", "[0-9]+"] {
                let re = regex_support::compile(pattern).unwrap();
                let expected = re.find(text)
                    .map_or(Value::Null, |matched| Value::Text(matched.as_str().to_owned()));
                assert_eq!(evaluate(&[
                    Value::Text(text.to_owned()), Value::Text(pattern.to_owned()),
                ]).unwrap(), expected, "{text:?} {pattern}");
            }
        }
    }
}


#[cfg(test)]
mod unicode_literal_substring_optimization_tests {
    use super::*;

    #[test]
    fn unicode_literal_results_match_regex_engine() {
        for text in ["", "é🙂東京東京", "aαβb", "café", "東京abc", "αβγ αβγ"] {
            for pattern in ["é", "東京", "αβ", "café", "éé", "é+", "^東京", "[0-9]+"] {
                let expected = regex_support::compile(pattern).unwrap().find(text)
                    .map_or(Value::Null, |found| Value::Text(found.as_str().to_owned()));
                assert_eq!(evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap(), expected, "text={text:?}, pattern={pattern:?}");
            }
        }
        assert_eq!(evaluate(&[Value::Null, Value::Text("東京".into())]).unwrap(),
            Value::Null);
    }
}
