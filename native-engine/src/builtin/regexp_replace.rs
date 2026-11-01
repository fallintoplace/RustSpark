use super::{Function, regex_support};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(&types[..3])?;
    if types.len() == 4 && types[3] != DataType::Integer {
        return Err(Error::Type("REGEXP_REPLACE position must be an integer".into()));
    }
    Ok(DataType::Text)
}

// Capture substitution is bounded before each append. In contrast,
// captures.expand materializes a potentially huge intermediate string before
// a caller can check whether the result exceeds the native byte budget.
const MAX_REGEX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

fn push_bounded(output: &mut String, text: &str) -> Result<()> {
    if output.len().checked_add(text.len())
        .is_none_or(|total| total > MAX_REGEX_OUTPUT_BYTES) {
        return Err(Error::Execution("regex output exceeds native 16 MiB budget".into()));
    }
    output.push_str(text);
    Ok(())
}

// Match the regex crate's $N, $name, ${name}, and $$ replacement grammar.
fn append_bounded_expansion(
    captures: &regex::Captures<'_>, mut replacement: &str, output: &mut String,
) -> Result<()> {
    while let Some(position) = replacement.find('$') {
        push_bounded(output, &replacement[..position])?;
        replacement = &replacement[position..];
        if let Some(rest) = replacement.strip_prefix("$$") {
            push_bounded(output, "$")?;
            replacement = rest;
            continue;
        }
        let (name, remaining) = if let Some(rest) = replacement.strip_prefix("${") {
            if let Some(close) = rest.find('}') {
                (&rest[..close], &rest[close + 1..])
            } else {
                push_bounded(output, "$")?;
                replacement = &replacement[1..];
                continue;
            }
        } else {
            let width = replacement[1..].bytes()
                .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                .count();
            if width == 0 {
                push_bounded(output, "$")?;
                replacement = &replacement[1..];
                continue;
            }
            (&replacement[1..width + 1], &replacement[width + 1..])
        };
        let captured = if let Ok(index) = name.parse::<usize>() {
            captures.get(index)
        } else {
            captures.name(name)
        };
        if let Some(value) = captured {
            push_bounded(output, value.as_str())?;
        }
        replacement = remaining;
    }
    push_bounded(output, replacement)
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
    for captures in compiled.captures_iter(suffix) {
        let matched = captures.get(0).expect("regex captures always contain the whole match");
        push_bounded(&mut result, &suffix[consumed..matched.start()])?;
        append_bounded_expansion(&captures, replacement, &mut result)?;
        consumed = matched.end();
    }
    push_bounded(&mut result, &suffix[consumed..])?;
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
mod bounded_capture_template_expansion_tests {
    use super::*;

    #[test]
    fn templates_match_regex_capture_expansion_semantics() {
        let re = regex_support::compile(r"(?P<first>[a-z]+)-(?P<num>[0-9]+)").unwrap();
        let captures = re.captures("abc-12").unwrap();
        for template in [
            "", "$$", "$0", "$1", "$2", "$first", "${first}",
            "$num:$first", "$1x", "${1}x", "$unknown", "$999",
            "$", "$?", "$-x", "$$1", "$$$1", "${unknown}",
            "${not-closed", "é🙂$num", "$first_${num}",
        ] {
            let mut expected = String::new();
            captures.expand(template, &mut expected);
            let mut actual = String::new();
            append_bounded_expansion(&captures, template, &mut actual).unwrap();
            assert_eq!(actual, expected, "template={template:?}");
        }
    }

    #[test]
    fn repeated_large_capture_cannot_expand_beyond_output_budget() {
        let input = "x".repeat(1_000_000);
        let args = [
            Value::Text(input.clone()), Value::Text("(x+)".into()),
            Value::Text("$1".repeat(30)),
        ];
        assert!(matches!(evaluate(&args), Err(Error::Execution(_))));
        let allowed = [
            Value::Text(input), Value::Text("(x+)".into()),
            Value::Text("$1".repeat(10)),
        ];
        let Value::Text(value) = evaluate(&allowed).unwrap() else { panic!("expected TEXT"); };
        assert_eq!(value.len(), 10_000_000);
        assert!(value.bytes().all(|byte| byte == b'x'));
    }

    #[test]
    fn dollar_escapes_and_empty_groups_preserve_exact_output() {
        let output = evaluate(&[
            Value::Text("ab-12".into()),
            Value::Text("(?P<first>[a-z]+)-(?P<num>[0-9]+)".into()),
            Value::Text("$$:${num}/$first/$missing".into()),
        ]).unwrap();
        assert_eq!(output, Value::Text("$:12/ab/".into()));
    }
}
