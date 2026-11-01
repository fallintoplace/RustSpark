use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    // An empty Rust regex matches at each UTF-8 character boundary.
    if pattern.is_empty() {
        let characters = if text.is_ascii() { text.len() } else { text.chars().count() };
        let count = characters.checked_add(1)
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(|| Error::Execution("REGEXP_COUNT overflow".into()))?;
        return Ok(Value::Integer(count));
    }
    // An anchored identifier literal can match at most once.
    if let Some(prefix) = pattern.strip_prefix('^') {
        if prefix.len() <= 4095 && prefix.bytes().all(|byte|
            byte.is_ascii_alphanumeric() || byte == b'_') {
            return Ok(Value::Integer(i64::from(text.starts_with(prefix))));
        }
    }
    if pattern == "[0-9]" || pattern == "[0-9]+" {
        // These exact classes match ASCII bytes, not Unicode-wide \\d.
        // A single scan can count all digits or maximal digit runs.
        let runs = pattern == "[0-9]+";
        let mut count = 0usize;
        let mut previous_digit = false;
        for byte in text.bytes() {
            let digit = byte.is_ascii_digit();
            if digit && (!runs || !previous_digit) { count += 1; }
            previous_digit = digit;
        }
        return i64::try_from(count).map(Value::Integer)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()));
    }
    if pattern == ".+" {
        // Greedy dot-plus matches each maximal nonempty run excluding LF.
        // Counting those runs avoids compiling or traversing regex matches.
        let runs = text.split('\n').filter(|part| !part.is_empty()).count();
        return i64::try_from(runs).map(Value::Integer)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()));
    }
    if pattern == "." {
        // Rust regex dot matches a Unicode scalar except LF by default.
        let count = text.chars().filter(|&ch| ch != '\n').count();
        return Ok(Value::Integer(i64::try_from(count)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?));
    }
    if pattern.len() == 1 && pattern.as_bytes()[0].is_ascii_alphanumeric() {
        let needle = pattern.as_bytes()[0];
        let count = text.bytes().filter(|&byte| byte == needle).count();
        return Ok(Value::Integer(i64::try_from(count)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?));
    }
    if pattern.len() <= 4096 && pattern.bytes().all(|byte|
        byte.is_ascii_alphanumeric() || byte == b'_') {
        let count = text.match_indices(pattern).count();
        return Ok(Value::Integer(i64::try_from(count)
            .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?));
    }
    let re = regex_support::compile(pattern)?;
    let count = re.find_iter(text).count();
    Ok(Value::Integer(i64::try_from(count)
        .map_err(|_| Error::Execution("REGEXP_COUNT overflow".into()))?))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_count", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_nonoverlapping_unicode_regex_matches() {
        assert_eq!(evaluate(&[Value::Text("a1 b2 c3".into()),
            Value::Text("[0-9]".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("abc".into()),
            Value::Text("[0-9]".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Text("é🙂".into()),
            Value::Text("".into())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Text("".into()),
            Value::Text("".into())]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod literal_ascii_regexp_count_tests {
    use super::*;

    #[test]
    fn counts_literals_without_changing_regex_metacharacters() {
        let run = |text: &str, pattern: &str| evaluate(&[
            Value::Text(text.into()), Value::Text(pattern.into())
        ]).unwrap();
        assert_eq!(run("banana", "a"), Value::Integer(3));
        assert_eq!(run("é1🙂1", "1"), Value::Integer(2));
        assert_eq!(run("Aaa", "a"), Value::Integer(2));
        assert_eq!(run("a.a", "."), Value::Integer(3));
        assert_eq!(run("aaa", "a+"), Value::Integer(1));
        assert_eq!(run("é🙂", ""), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Null, Value::Text("a".into())]).unwrap(),
            Value::Null);
    }
}

#[cfg(test)]
mod ascii_empty_regexp_count_tests {
    use super::*;
    #[test]
    fn empty_pattern_counts_utf8_character_boundaries() {
        let ascii = "x".repeat(100_000);
        assert_eq!(evaluate(&[Value::Text(ascii), Value::Text(String::new())]).unwrap(), Value::Integer(100_001));
        assert_eq!(evaluate(&[Value::Text("é🙂".into()), Value::Text(String::new())]).unwrap(), Value::Integer(3));
        assert_eq!(evaluate(&[Value::Null, Value::Text(String::new())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod literal_regex_count_tests {
    use super::*;

    #[test]
    fn counts_non_overlapping_literals_like_regex() {
        for (text, pattern) in [
            ("aaaaa", "aa"), ("foofoo", "foo"), ("éfoo_foo", "foo"),
            ("abc", "xyz"), ("", "a"),
        ] {
            let args = [Value::Text(text.into()), Value::Text(pattern.into())];
            let expected = regex_support::compile(pattern).unwrap().find_iter(text).count();
            assert_eq!(evaluate(&args).unwrap(), Value::Integer(expected as i64));
        }
    }
}

#[cfg(test)]
mod dot_pattern_count_tests {
    use super::*;

    #[test]
    fn dot_counts_non_newline_unicode_scalars_like_regex() {
        for text in ["", "\n", "\n\n", "é🙂\nX", "\r\n", "foo\u{2028}bar"] {
            let expected = regex_support::compile(".").unwrap().find_iter(text).count();
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
mod anchored_literal_count_fast_path_tests {
    use super::*;

    #[test]
    fn anchored_counts_match_the_regex_engine() {
        for text in ["", "foo", "foobar", "xfoo", "\nfoo", "éfoo", "foo\nfoo"] {
            for pattern in ["^", "^foo", "^foo_bar", "^1", "^FOO", "^f.o", "(?m)^foo"] {
                let actual = evaluate(&[
                    Value::Text(text.into()), Value::Text(pattern.into()),
                ]).unwrap();
                let expected = regex_support::compile(pattern).unwrap().find_iter(text).count();
                assert_eq!(actual, Value::Integer(expected as i64),
                    "pattern={pattern:?} text={text:?}");
            }
        }
    }
}


#[cfg(test)]
mod dot_plus_regexp_count_differential_tests {
    use super::*;

    #[test]
    fn greedy_runs_match_regex_find_iter_for_unicode_and_newlines() {
        let regex = regex_support::compile(".+").unwrap();
        for input in ["", "\n", "abc", "abc\ndef", "\n\nabc\n\n", "é🙂\n東京", "\r\n", "A\r\nB\nC"] {
            assert_eq!(evaluate(&[
                Value::Text(input.into()), Value::Text(".+".into()),
            ]).unwrap(), Value::Integer(regex.find_iter(input).count() as i64), "{input:?}");
        }
        let huge = "é🙂x\n".repeat(40_000);
        assert_eq!(evaluate(&[
            Value::Text(huge), Value::Text(".+".into()),
        ]).unwrap(), Value::Integer(40_000));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text(".+".into()),
        ]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod regex_digit_class_count_regressions {
    use super::*;

    #[test]
    fn digit_class_fast_path_matches_regex_on_utf8_and_newlines() {
        let long_run = format!("{}-{}-{}", "0".repeat(25_000),
            "a".repeat(1_000), "12345".repeat(8_000));
        let samples = ["", "123", "a12b345", "é12🙂34", "١٢٣",
            "line1\n22\nend333", "123-456-000", long_run.as_str()];
        for input in samples {
            for pattern in ["[0-9]", "[0-9]+"] {
                let regex = regex_support::compile(pattern).unwrap();
                let expected = regex.find_iter(input).count() as i64;
                assert_eq!(evaluate(&[
                    Value::Text(input.to_owned()), Value::Text(pattern.to_owned()),
                ]).unwrap(), Value::Integer(expected), "{input:?}, {pattern}");
            }
        }
    }
}
