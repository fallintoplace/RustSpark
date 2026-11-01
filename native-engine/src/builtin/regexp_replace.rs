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
        // The number of Unicode scalars cannot exceed the byte length.
        if offset >= text.len() {
            text.len()
        } else if text.is_ascii() {
            offset
        } else {
            text.char_indices().nth(offset).map_or(text.len(), |(index, _)| index)
        }
    } else { 0 };
    let suffix = &text[start..];

    // For literal replacement strings, ASCII digit classes need no compiled
    // regex or capture buffers. Every match begins and ends at UTF-8 boundaries.
    if (pattern == "[0-9]" || pattern == "[0-9]+") && !replacement.contains('    // A dollar in replacement requires the regex capture-expansion path.
    if !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.chars().all(|ch| ch.is_alphanumeric() || ch == '_') &&
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

#[cfg(test)]
mod oversized_replace_position_regressions {
    use super::*;

    #[test]
    fn huge_offsets_match_existing_unicode_position_semantics() {
        let run = |text: &str, position: i64| {
            evaluate(&[
                Value::Text(text.into()), Value::Text("x".into()),
                Value::Text("Q".into()), Value::Integer(position),
            ]).unwrap()
        };
        let long = "x".repeat(1024);
        for text in ["", "abc", "é🙂x", long.as_str()] {
            let expected = Value::Text(text.into());
            assert_eq!(run(text, i64::MAX), expected);
            assert_eq!(run(text, text.len() as i64 + 1), expected);
        }
        assert_eq!(run("é🙂x", 3), Value::Text("é🙂Q".into()));
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("(".into()),
            Value::Text("Q".into()), Value::Integer(i64::MAX),
        ]).is_err());
    }
}

#[cfg(test)]
mod unicode_literal_regex_fast_path_tests {
    use super::*;
    use regex::Regex;

    fn call(input: &str, pattern: &str, replacement: &str, position: Option<i64>) -> Result<String> {
        let mut args = vec![
            Value::Text(input.into()), Value::Text(pattern.into()),
            Value::Text(replacement.into()),
        ];
        if let Some(position) = position { args.push(Value::Integer(position)); }
        let Value::Text(output) = evaluate(&args)? else { panic!("expected text") };
        Ok(output)
    }

    #[test]
    fn unicode_literals_match_regex_reference_at_every_character_position() {
        for pattern in ["é", "東京", "🙂", "αβ", "１２３", "界", "café"] {
            let input = format!("before{pattern}middle{pattern}after{pattern}");
            for replacement in ["x", "é🙂", "", "abcdef"] {
                for start in [1, 2, 7, 1000] {
                    let offset = input.char_indices().nth(start - 1)
                        .map_or(input.len(), |(index, _)| index);
                    let expected = format!("{}{}", &input[..offset],
                        Regex::new(pattern).unwrap()
                            .replace_all(&input[offset..], replacement));
                    assert_eq!(call(&input, pattern, replacement, Some(start as i64)).unwrap(),
                        expected, "pattern={pattern:?}, replacement={replacement:?}, start={start}");
                }
            }
        }
    }

    #[test]
    fn metacharacters_and_capture_expansion_still_use_regex_engine() {
        assert_eq!(call("éé", "é", "$0!", None).unwrap(), "é!é!");
        assert_eq!(call("éé", "(é)", "${1}!", None).unwrap(), "é!é!");
        assert_eq!(call("aé🙂a", "é🙂", "?", None).unwrap(), "a?a");
        assert_eq!(call("abc", "a.c", "yes", None).unwrap(), "yes");
        assert!(call("é", "é", &"x".repeat(1_048_577), None).is_err());
    }
}
) {
        const MAX_OUTPUT: usize = 16 * 1024 * 1024;
        let mut result = String::with_capacity(text.len());
        result.push_str(&text[..start]);
        let bytes = suffix.as_bytes();
        let mut consumed = 0usize;
        let mut index = 0usize;
        while index < bytes.len() {
            if !bytes[index].is_ascii_digit() {
                index += 1;
                continue;
            }
            let begin = index;
            index += 1;
            if pattern == "[0-9]+" {
                while index < bytes.len() && bytes[index].is_ascii_digit() { index += 1; }
            }
            let unchanged = &suffix[consumed..begin];
            result.len().checked_add(unchanged.len())
                .and_then(|length| length.checked_add(replacement.len()))
                .filter(|&length| length <= MAX_OUTPUT)
                .ok_or_else(|| Error::Execution("regex output exceeds native 16 MiB budget".into()))?;
            result.push_str(unchanged);
            result.push_str(replacement);
            consumed = index;
        }
        let remaining = &suffix[consumed..];
        if result.len().checked_add(remaining.len())
            .is_none_or(|length| length > MAX_OUTPUT) {
            return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
        }
        result.push_str(remaining);
        return Ok(Value::Text(result));
    }

    // Literal Unicode patterns do not need regex compilation or captures.
    // A dollar in replacement requires the regex capture-expansion path.
    if !pattern.is_empty() && pattern.len() <= 4096 &&
        pattern.chars().all(|ch| ch.is_alphanumeric() || ch == '_') &&
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

#[cfg(test)]
mod oversized_replace_position_regressions {
    use super::*;

    #[test]
    fn huge_offsets_match_existing_unicode_position_semantics() {
        let run = |text: &str, position: i64| {
            evaluate(&[
                Value::Text(text.into()), Value::Text("x".into()),
                Value::Text("Q".into()), Value::Integer(position),
            ]).unwrap()
        };
        let long = "x".repeat(1024);
        for text in ["", "abc", "é🙂x", long.as_str()] {
            let expected = Value::Text(text.into());
            assert_eq!(run(text, i64::MAX), expected);
            assert_eq!(run(text, text.len() as i64 + 1), expected);
        }
        assert_eq!(run("é🙂x", 3), Value::Text("é🙂Q".into()));
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("(".into()),
            Value::Text("Q".into()), Value::Integer(i64::MAX),
        ]).is_err());
    }
}

#[cfg(test)]
mod unicode_literal_regex_fast_path_tests {
    use super::*;
    use regex::Regex;

    fn call(input: &str, pattern: &str, replacement: &str, position: Option<i64>) -> Result<String> {
        let mut args = vec![
            Value::Text(input.into()), Value::Text(pattern.into()),
            Value::Text(replacement.into()),
        ];
        if let Some(position) = position { args.push(Value::Integer(position)); }
        let Value::Text(output) = evaluate(&args)? else { panic!("expected text") };
        Ok(output)
    }

    #[test]
    fn unicode_literals_match_regex_reference_at_every_character_position() {
        for pattern in ["é", "東京", "🙂", "αβ", "１２３", "界", "café"] {
            let input = format!("before{pattern}middle{pattern}after{pattern}");
            for replacement in ["x", "é🙂", "", "abcdef"] {
                for start in [1, 2, 7, 1000] {
                    let offset = input.char_indices().nth(start - 1)
                        .map_or(input.len(), |(index, _)| index);
                    let expected = format!("{}{}", &input[..offset],
                        Regex::new(pattern).unwrap()
                            .replace_all(&input[offset..], replacement));
                    assert_eq!(call(&input, pattern, replacement, Some(start as i64)).unwrap(),
                        expected, "pattern={pattern:?}, replacement={replacement:?}, start={start}");
                }
            }
        }
    }

    #[test]
    fn metacharacters_and_capture_expansion_still_use_regex_engine() {
        assert_eq!(call("éé", "é", "$0!", None).unwrap(), "é!é!");
        assert_eq!(call("éé", "(é)", "${1}!", None).unwrap(), "é!é!");
        assert_eq!(call("aé🙂a", "é🙂", "?", None).unwrap(), "a?a");
        assert_eq!(call("abc", "a.c", "yes", None).unwrap(), "yes");
        assert!(call("é", "é", &"x".repeat(1_048_577), None).is_err());
    }
}

#[cfg(test)]
mod digit_class_replace_regressions {
    use super::*;

    #[test]
    fn direct_digit_replacement_matches_regex_and_unicode_positions() {
        for input in ["", "abc123z45", "é🙂9東京20x", "١٢٣", "x0\n77", "999"] {
            for pattern in ["[0-9]", "[0-9]+"] {
                for replacement in ["", "X", "é🙂"] {
                    for position in [1_i64, 2, 5] {
                        let byte_offset = input.char_indices().nth((position - 1) as usize)
                            .map_or(input.len(), |(index, _)| index);
                        let suffix = &input[byte_offset..];
                        let expected = format!("{}{}",
                            &input[..byte_offset],
                            regex_support::compile(pattern).unwrap().replace_all(suffix, replacement));
                        let actual = evaluate(&[
                            Value::Text(input.into()),
                            Value::Text(pattern.into()),
                            Value::Text(replacement.into()),
                            Value::Integer(position),
                        ]).unwrap();
                        assert_eq!(actual, Value::Text(expected), "input={input:?} pattern={pattern}");
                    }
                }
            }
        }
    }

    #[test]
    fn digit_replacement_still_enforces_output_budget() {
        let large = "9".repeat(6 * 1024 * 1024);
        assert!(evaluate(&[
            Value::Text(large), Value::Text("[0-9]".into()), Value::Text("abc".into())
        ]).is_err());
        assert_eq!(evaluate(&[
            Value::Text("99".into()), Value::Text("[0-9]+".into()), Value::Text("$0".into())
        ]).unwrap(), Value::Text("99".into()));
    }
}
