use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(&types[..3])?;
    if types.len() == 4 && types[3] != DataType::Integer {
        return Err(Error::Type("REGEXP_REPLACE position must be an integer".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(pattern) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(replacement) = super::string(&args[2])? else { return Ok(Value::Null); };
    regex_support::check_input(text)?;
    if replacement.len() > 1024 * 1024 {
        return Err(Error::Execution("regex replacement exceeds 1 MiB".into()));
    }
    let start = if args.len() == 4 {
        let Some(position) = super::integer(&args[3])? else { return Ok(Value::Null); };
        if position < 1 {
            return Err(Error::Execution("REGEXP_REPLACE position must be positive".into()));
        }
        let offset = usize::try_from(position - 1).unwrap_or(usize::MAX);
        if text.is_ascii() {
            offset.min(text.len())
        } else {
            text.char_indices().nth(offset).map_or(text.len(), |(index, _)| index)
        }
    } else { 0 };
    let suffix = &text[start..];

    // Literal ASCII patterns do not need regex compilation or captures.
    // A dollar in replacement requires the regex capture-expansion path.
    if !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') &&
        !replacement.contains('$') {
        // Scan once, appending whole unchanged UTF-8 spans. Check the
        // output budget incrementally to reject expansion before allocating it.
        let mut result = String::with_capacity(text.len());
        result.push_str(&text[..start]);
        let mut consumed = 0usize;
        for (index, _) in suffix.match_indices(pattern) {
            let unchanged = &suffix[consumed..index];
            let next_len = result.len().checked_add(unchanged.len())
                .and_then(|len| len.checked_add(replacement.len()))
                .filter(|&len| len <= 16 * 1024 * 1024)
                .ok_or_else(|| Error::Execution("regex output exceeds native 16 MiB budget".into()))?;
            result.push_str(unchanged);
            result.push_str(replacement);
            debug_assert_eq!(result.len(), next_len);
            consumed = index + pattern.len();
        }
        let remaining = &suffix[consumed..];
        if result.len().checked_add(remaining.len())
            .is_none_or(|len| len > 16 * 1024 * 1024) {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        result.push_str(remaining);
        return Ok(Value::Text(result));
    }

    let compiled = regex_support::compile(pattern)?;
    let mut result = String::with_capacity(text.len());
    result.push_str(&text[..start]);
    let mut consumed = 0;
    let mut expanded = String::new();
    for captures in compiled.captures_iter(suffix) {
        let matched = captures.get(0).expect("regex captures always contain the whole match");
        let unchanged = &suffix[consumed..matched.start()];
        expanded.clear();
        captures.expand(replacement, &mut expanded);
        if result.len().checked_add(unchanged.len())
            .and_then(|len| len.checked_add(expanded.len()))
            .is_none_or(|len| len > 16 * 1024 * 1024) {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        result.push_str(unchanged);
        result.push_str(&expanded);
        consumed = matched.end();
    }
    let remaining = &suffix[consumed..];
    if result.len().checked_add(remaining.len())
        .is_none_or(|len| len > 16 * 1024 * 1024) {
        return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
    }
    result.push_str(remaining);
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "regexp_replace", min_args: 3, max_args: 4,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replaces_multiple_matches_and_capture_groups() {
        assert_eq!(evaluate(&[
            Value::Text("id:12 id:34".into()),
            Value::Text("id:([0-9]+)".into()),
            Value::Text("ref-$1".into()),
        ]).unwrap(), Value::Text("ref-12 ref-34".into()));
        assert!(evaluate(&[
            Value::Text("x".into()), Value::Text("(".into()), Value::Text("y".into())
        ]).is_err());
    }
}

#[cfg(test)]
mod spark_position_regressions {
    use super::*;

    #[test]
    fn position_preserves_prefix_and_replaces_only_suffix() {
        let args = |position| [
            Value::Text("ab12cd34".into()), Value::Text("[0-9]+".into()),
            Value::Text("x".into()), Value::Integer(position),
        ];
        assert_eq!(evaluate(&args(5)).unwrap(), Value::Text("ab12cdx".into()));
        assert_eq!(evaluate(&args(1)).unwrap(), Value::Text("abxcdx".into()));
        assert_eq!(evaluate(&args(99)).unwrap(), Value::Text("ab12cd34".into()));
        assert!(evaluate(&args(0)).is_err());
        assert!(output(&[DataType::Text, DataType::Text, DataType::Text, DataType::Text]).is_err());
    }

    #[test]
    fn position_counts_unicode_characters() {
        assert_eq!(evaluate(&[
            Value::Text("éé11".into()), Value::Text("1".into()),
            Value::Text("x".into()), Value::Integer(3),
        ]).unwrap(), Value::Text("ééxx".into()));
    }
}

#[cfg(test)]
mod bounded_incremental_regex_replace_tests {
    use super::*;
    #[test]
    fn rejects_expanding_outputs_before_allocating_them_all() {
        let input = Value::Text("a".repeat(1_000));
        let replacement = Value::Text("x".repeat(100_000));
        assert!(evaluate(&[input, Value::Text("a".into()), replacement]).is_err());
    }
    #[test]
    fn preserves_captures_zero_width_matches_and_positions() {
        assert_eq!(evaluate(&[Value::Text("ab12".into()), Value::Text("([a-z]+)([0-9]+)".into()),
            Value::Text("$2/$1".into())]).unwrap(), Value::Text("12/ab".into()));
        assert_eq!(evaluate(&[Value::Text("ab".into()), Value::Text("".into()),
            Value::Text("-".into())]).unwrap(), Value::Text("-a-b-".into()));
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("z".into()),
            Value::Text("x".into())]).unwrap(), Value::Text("abc".into()));
    }
}

#[cfg(test)]
mod expansion_buffer_reuse_tests {
    use super::*;

    #[test]
    fn multiple_captures_expand_independently() {
        let args = [
            Value::Text("a1 b2 c3".into()),
            Value::Text(r"([a-z])([0-9])".into()),
            Value::Text("$2$1".into()),
        ];
        assert_eq!(evaluate(&args).unwrap(), Value::Text("1a 2b 3c".into()));
    }
}

#[cfg(test)]
mod literal_regexp_replace_fast_path_tests {
    use super::*;

    fn run(text: &str, pattern: &str, replacement: &str, start: Option<i64>) -> Value {
        let mut args = vec![
            Value::Text(text.into()),
            Value::Text(pattern.into()),
            Value::Text(replacement.into()),
        ];
        if let Some(start) = start { args.push(Value::Integer(start)); }
        evaluate(&args).unwrap()
    }

    #[test]
    fn literal_patterns_match_regex_semantics_for_unicode_and_positions() {
        assert_eq!(run("aéab🙂ab", "ab", "xy", None),
            Value::Text("aéxy🙂xy".into()));
        assert_eq!(run("aéab🙂ab", "ab", "x", Some(4)),
            Value::Text("aéab🙂x".into()));
        assert_eq!(run("aaaaa", "aa", "X", None),
            Value::Text("XXa".into()));
        assert_eq!(run("hello", "missing", "x", None),
            Value::Text("hello".into()));
        assert_eq!(run("abc", "abc", "", None),
            Value::Text(String::new()));
    }

    #[test]
    fn dollars_and_metacharacters_keep_regex_expansion() {
        assert_eq!(run("ab", "(a)(b)", "$2$1", None),
            Value::Text("ba".into()));
        assert_eq!(run("a1a2", "a", "$$", None),
            Value::Text("$1$2".into()));
        assert_eq!(run("aa", "a+", "X", None),
            Value::Text("X".into()));
    }

    #[test]
    fn literal_growth_cannot_exceed_output_budget() {
        let args = [
            Value::Text("a".repeat(1024 * 1024)),
            Value::Text("a".into()),
            Value::Text("z".repeat(17)),
        ];
        assert!(matches!(evaluate(&args), Err(Error::Execution(_))));
    }
}

#[cfg(test)]
mod literal_regex_single_pass_tests {
    use super::*;

    #[test]
    fn handles_large_mixed_unicode_text_with_nonoverlapping_matches() {
        let input = format!("{}aabaa{}", "é🙂".repeat(3000), "界".repeat(3000));
        let expected = input.replace("aa", "XYZ");
        assert_eq!(evaluate(&[
            Value::Text(input), Value::Text("aa".into()), Value::Text("XYZ".into())
        ]).unwrap(), Value::Text(expected));
    }

    #[test]
    fn accepts_exact_budget_and_rejects_overflow() {
        let block = "z".repeat(1024 * 1024);
        let accepted = evaluate(&[
            Value::Text("a".repeat(16)), Value::Text("a".into()),
            Value::Text(block.clone()),
        ]).unwrap();
        let Value::Text(accepted) = accepted else { unreachable!() };
        assert_eq!(accepted.len(), 16 * 1024 * 1024);
        assert!(matches!(evaluate(&[
            Value::Text("a".repeat(17)), Value::Text("a".into()),
            Value::Text(block),
        ]), Err(Error::Execution(_))));
    }

    #[test]
    fn position_and_regex_capture_fallback_remain_unchanged() {
        assert_eq!(evaluate(&[
            Value::Text("éabcabc".into()), Value::Text("abc".into()),
            Value::Text("x".into()), Value::Integer(5),
        ]).unwrap(), Value::Text("éabcx".into()));
        assert_eq!(evaluate(&[
            Value::Text("abcabc".into()), Value::Text("(abc)".into()),
            Value::Text("$1!".into()),
        ]).unwrap(), Value::Text("abc!abc!".into()));
    }
}

#[cfg(test)]
mod single_pass_literal_replace_regressions {
    use super::*;

    #[test]
    fn literal_path_matches_regex_for_overlapping_and_unicode_inputs() {
        for (source, pattern, replacement) in [
            ("aaaaa", "aa", "X"),
            ("é🙂ab-éab", "ab", "界"),
            ("no matches", "xyz", "🙂"),
            ("hello_hello", "hello", ""),
        ] {
            let expected = regex_support::compile(pattern).unwrap()
                .replace_all(source, replacement).to_string();
            assert_eq!(evaluate(&[Value::Text(source.into()),
                Value::Text(pattern.into()), Value::Text(replacement.into())]).unwrap(),
                Value::Text(expected));
        }
        assert_eq!(evaluate(&[
            Value::Text("éab🙂ab".into()), Value::Text("ab".into()),
            Value::Text("x".into()), Value::Integer(4),
        ]).unwrap(), Value::Text("éab🙂x".into()));
    }

    #[test]
    fn incremental_growth_still_rejects_oversized_output() {
        assert!(matches!(evaluate(&[
            Value::Text("a".repeat(1_000_000)), Value::Text("a".into()),
            Value::Text("z".repeat(17)),
        ]), Err(Error::Execution(_))));
    }
}

#[cfg(test)]
mod ascii_regex_replace_position_fast_path_tests {
    use super::*;

    #[test]
    fn large_ascii_positions_match_one_based_unicode_semantics() {
        let input = "a".repeat(30_000);
        let actual = evaluate(&[
            Value::Text(input.clone()), Value::Text("a".into()),
            Value::Text("b".into()), Value::Integer(30_000),
        ]).unwrap();
        assert_eq!(actual, Value::Text(format!("{}b", "a".repeat(29_999))));
        assert_eq!(evaluate(&[
            Value::Text(input.clone()), Value::Text("a".into()),
            Value::Text("b".into()), Value::Integer(i64::MAX),
        ]).unwrap(), Value::Text(input));
        assert_eq!(evaluate(&[
            Value::Text("é🙂abc".into()), Value::Text("a".into()),
            Value::Text("X".into()), Value::Integer(3),
        ]).unwrap(), Value::Text("é🙂Xbc".into()));
    }
}
