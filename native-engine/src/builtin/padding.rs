use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const MAX_CHARS: usize = 1_000_000;

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Integer ||
        (types.len() == 3 && types[2] != DataType::Text) {
        return Err(Error::Type("LPAD/RPAD expects TEXT, INTEGER, optional TEXT".into()));
    }
    Ok(DataType::Text)
}
fn pad(args: &[Value], left: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let Some(len) = super::integer(&args[1])? else { return Ok(Value::Null) };
    let target = usize::try_from(len.max(0))
        .map_err(|_| Error::Execution("LPAD/RPAD length overflow".into()))?;
    if target > MAX_CHARS {
        return Err(Error::Execution("LPAD/RPAD length exceeds native output budget".into()));
    }
    let pad = if args.len() == 3 {
        let Some(value) = super::string(&args[2])? else { return Ok(Value::Null) };
        value
    } else { " " };
    let ascii = text.is_ascii();
    let size = super::char_length::character_count(text);
    if size >= target {
        if ascii { return Ok(Value::Text(text[..target].to_owned())); }
        return Ok(Value::Text(text.chars().take(target).collect()));
    }
    if pad.is_empty() { return Ok(Value::Text(text.to_owned())); }
    let mut chars = pad.chars();
    let first = chars.next();
    let one_char = first.filter(|_| chars.next().is_none());
    // Each ASCII pad scalar is one output byte, regardless of the pattern
    // length. The old estimate over-reserved for multi-byte ASCII patterns.
    let bytes_per_char = if pad.is_ascii() { 1 } else { pad.len().min(4) };
    let mut result = String::with_capacity(
        text.len().saturating_add((target - size).saturating_mul(bytes_per_char)),
    );
    if !left { result.push_str(text); }
    if let Some(character) = one_char {
        for _ in 0..(target - size) { result.push(character); }
    } else if pad.is_ascii() {
        // For multi-character ASCII patterns, copy whole runs using
        // push_str rather than decoding and appending each scalar.
        let padding = target - size;
        let full_runs = padding / pad.len();
        for _ in 0..full_runs {
            result.push_str(pad);
        }
        result.push_str(&pad[..padding % pad.len()]);
    } else {
        // Repeat whole UTF-8 patterns instead of pushing every Unicode
        // scalar individually. The final partial run ends on a char boundary.
        let pattern_chars = pad.chars().count();
        let padding = target - size;
        let complete = padding / pattern_chars;
        for _ in 0..complete {
            result.push_str(pad);
        }
        let remainder = padding % pattern_chars;
        if remainder != 0 {
            let end = pad.char_indices().nth(remainder)
                .expect("partial pattern has an interior UTF-8 boundary").0;
            result.push_str(&pad[..end]);
        }
    }
    if left { result.push_str(text); }
    Ok(Value::Text(result))
}
fn lpad(args: &[Value]) -> Result<Value> { pad(args, true) }
fn rpad(args: &[Value]) -> Result<Value> { pad(args, false) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "lpad", min_args: 2, max_args: 3, result_type: output, call: lpad },
    Function { name: "rpad", min_args: 2, max_args: 3, result_type: output, call: rpad },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pads_unicode_by_scalar_value_not_bytes() {
        assert_eq!(lpad(&[Value::Text("ab".into()), Value::Integer(4), Value::Text("🙂".into())]).unwrap(), Value::Text("🙂🙂ab".into()));
        assert_eq!(rpad(&[Value::Text("a".into()), Value::Integer(3), Value::Text("xy".into())]).unwrap(), Value::Text("axy".into()));
        assert_eq!(lpad(&[Value::Text("界面".into()), Value::Integer(1)]).unwrap(), Value::Text("界".into()));
        assert!(rpad(&[Value::Text("a".into()), Value::Integer(2_000_000)]).is_err());
        assert_eq!(lpad(&[Value::Text("hello".into()), Value::Integer(-1)]).unwrap(), Value::Text("".into()));
        assert_eq!(rpad(&[Value::Text("hello".into()), Value::Integer(i64::MIN)]).unwrap(), Value::Text("".into()));
    }
}

#[cfg(test)]
mod single_buffer_pad_perf_tests {
    use super::*;

    #[test]
    fn preserves_multi_character_padding_on_both_sides() {
        assert_eq!(lpad(&[
            Value::Text("é".into()), Value::Integer(6), Value::Text("ab".into())
        ]).unwrap(), Value::Text("ababaé".into()));
        assert_eq!(rpad(&[
            Value::Text("é".into()), Value::Integer(6), Value::Text("🙂a".into())
        ]).unwrap(), Value::Text("é🙂a🙂a🙂".into()));
    }
}

#[cfg(test)]
mod ascii_padding_slice_perf_tests {
    use super::*;
    #[test]
    fn truncates_ascii_without_changing_unicode_boundaries() {
        assert_eq!(lpad(&[Value::Text("abcdef".into()), Value::Integer(3)]).unwrap(), Value::Text("abc".into()));
        assert_eq!(rpad(&[Value::Text("abcdef".into()), Value::Integer(6)]).unwrap(), Value::Text("abcdef".into()));
        assert_eq!(lpad(&[Value::Text("é🙂x".into()), Value::Integer(2)]).unwrap(), Value::Text("é🙂".into()));
    }
}

#[cfg(test)]
mod empty_padding_spark_tests {
    use super::*;
    #[test]
    fn an_empty_pad_keeps_original_when_extension_is_requested() {
        assert_eq!(lpad(&[Value::Text("éab".into()), Value::Integer(5), Value::Text("".into())]).unwrap(), Value::Text("éab".into()));
        assert_eq!(rpad(&[Value::Text("éab".into()), Value::Integer(5), Value::Text("".into())]).unwrap(), Value::Text("éab".into()));
        assert_eq!(lpad(&[Value::Text("éab".into()), Value::Integer(2), Value::Text("".into())]).unwrap(), Value::Text("éa".into()));
        assert_eq!(rpad(&[Value::Null, Value::Integer(5), Value::Text("".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod single_character_padding_tests {
    use super::*;
    #[test]
    fn single_character_patterns_preserve_multibyte_output() {
        assert_eq!(lpad(&[Value::Text("x".into()), Value::Integer(4)]).unwrap(),
            Value::Text("   x".into()));
        assert_eq!(rpad(&[Value::Text("x".into()), Value::Integer(4), Value::Text("界".into())]).unwrap(),
            Value::Text("x界界界".into()));
        assert_eq!(rpad(&[Value::Text("x".into()), Value::Integer(4), Value::Text("ab".into())]).unwrap(),
            Value::Text("xaba".into()));
        let long_pattern = format!("🙂{}", "z".repeat(20_000));
        assert_eq!(rpad(&[Value::Text("x".into()), Value::Integer(3),
            Value::Text(long_pattern)]).unwrap(), Value::Text("x🙂z".into()));
    }
}

#[cfg(test)]
mod ascii_padding_capacity_estimate_tests {
    use super::*;
    #[test]
    fn pads_long_ascii_patterns_without_unicode_regressions() {
        let text = Value::Text("a".into());
        assert_eq!(rpad(&[text.clone(), Value::Integer(8),
            Value::Text("abcd".into())]).unwrap(), Value::Text("aabcdabc".into()));
        assert_eq!(lpad(&[text, Value::Integer(7),
            Value::Text("xyz".into())]).unwrap(), Value::Text("xyzxyza".into()));
        assert_eq!(rpad(&[Value::Text("é".into()), Value::Integer(4),
            Value::Text("🙂x".into())]).unwrap(), Value::Text("é🙂x🙂".into()));
    }
}

#[cfg(test)]
mod ascii_padding_run_copy_tests {
    use super::*;

    fn run(text: &str, width: i64, pad: &str, left: bool) -> Result<Value> {
        let args = [
            Value::Text(text.into()), Value::Integer(width), Value::Text(pad.into()),
        ];
        if left { lpad(&args) } else { rpad(&args) }
    }

    #[test]
    fn whole_and_partial_ascii_patterns_keep_exact_char_counts() {
        assert_eq!(run("X", 10, "abc", true).unwrap(),
            Value::Text("abcabcabcX".into()));
        assert_eq!(run("X", 9, "abc", false).unwrap(),
            Value::Text("Xabcabcab".into()));
        assert_eq!(run("🙂", 8, "ab", true).unwrap(),
            Value::Text("abababa🙂".into()));
        assert_eq!(run("X", 3, "abcd", false).unwrap(),
            Value::Text("Xab".into()));
        assert_eq!(run("🙂", 4, "é界", false).unwrap(),
            Value::Text("🙂é界é".into()));
    }

    #[test]
    fn long_ascii_pattern_appends_are_bounded_by_target_length() {
        let pattern = "0123456789abcdef";
        let result = run("end", 999_999, pattern, true).unwrap();
        let Value::Text(value) = result else { panic!("text result expected") };
        assert_eq!(value.len(), 999_999);
        assert!(value.ends_with("end"));
        assert!(value.starts_with(pattern));
        assert!(run("X", 1_000_001, "ab", true).is_err());
    }
}

#[cfg(test)]
mod unicode_padding_run_regressions {
    use super::*;

    #[test]
    fn bulk_unicode_patterns_match_character_cycle() {
        let source = "中";
        let pattern = "é🙂界";
        for target in [0, 1, 2, 3, 4, 5, 7, 10, 32, 255] {
            let extra = target.saturating_sub(1usize);
            let padding: String = pattern.chars().cycle().take(extra).collect();
            let left = format!("{padding}{source}");
            let right = format!("{source}{padding}");
            let args = [
                Value::Text(source.into()),
                Value::Integer(target as i64),
                Value::Text(pattern.into()),
            ];
            assert_eq!(lpad(&args).unwrap(), Value::Text(left));
            assert_eq!(rpad(&args).unwrap(), Value::Text(right));
        }
        let large = [
            Value::Text("é".into()),
            Value::Integer(10_001),
            Value::Text("🙂界".into()),
        ];
        let Value::Text(result) = rpad(&large).unwrap() else { panic!("text"); };
        assert_eq!(result.chars().count(), 10_001);
        assert!(result.starts_with("é🙂界🙂界"));
    }
}
