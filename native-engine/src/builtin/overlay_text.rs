use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] != DataType::Text || types[1] != DataType::Text ||
        types[2] != DataType::Integer ||
        (types.len() == 4 && types[3] != DataType::Integer) {
        return Err(Error::Type("OVERLAY requires text, replacement, start, optional length".into()));
    }
    Ok(DataType::Text)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(replacement) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(start) = super::integer(&args[2])? else { return Ok(Value::Null); };
    if start < 1 { return Err(Error::Execution("OVERLAY start must be positive".into())); }
    let count = if args.len() == 4 {
        let Some(count) = super::integer(&args[3])? else { return Ok(Value::Null); };
        if count < 0 { replacement.chars().count() }
        else { usize::try_from(count).unwrap_or(usize::MAX) }
    } else {
        replacement.chars().count()
    };
    const MAX_BYTES: usize = 16 * 1024 * 1024;
    if text.len() > MAX_BYTES || replacement.len() > MAX_BYTES {
        return Err(Error::Execution("OVERLAY input exceeds native 16 MiB budget".into()));
    }
    let index = usize::try_from(start - 1).unwrap_or(usize::MAX);
    // A byte count bounds the number of UTF-8 characters, so impossible
    // character positions need no iterator traversal.
    let ascii = text.is_ascii();
    let begin = if index >= text.len() { text.len() } else if ascii {
        // Byte and character offsets are identical for ASCII text.
        index
    } else {
        text.char_indices().nth(index).map_or(text.len(), |(byte, _)| byte)
    };
    let remaining = &text[begin..];
    let end = if count >= remaining.len() { text.len() } else if ascii {
        begin + count
    } else {
        remaining.char_indices().nth(count).map_or(text.len(), |(byte, _)| begin + byte)
    };
    let result_len = begin.checked_add(replacement.len())
        .and_then(|n| n.checked_add(text.len() - end))
        .filter(|&n| n <= MAX_BYTES)
        .ok_or_else(|| Error::Execution("OVERLAY output exceeds native 16 MiB budget".into()))?;
    let mut result = String::with_capacity(result_len);
    result.push_str(&text[..begin]);
    result.push_str(replacement);
    result.push_str(&text[end..]);
    Ok(Value::Text(result))
}


fn common_suffix_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn common_suffix(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
    if left == right {
        return Ok(Value::Text(left.to_owned()));
    }
    let mut start = left.len();
    for ((offset, first), (_, second)) in left.char_indices().rev().zip(right.char_indices().rev()) {
        if first != second { break; }
        start = offset;
    }
    Ok(Value::Text(left[start..].to_string()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "common_suffix", min_args: 2, max_args: 2, result_type: common_suffix_type, call: common_suffix },Function {
    name: "overlay", min_args: 3, max_args: 4,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replaces_unicode_segments_by_character_index() {
        assert_eq!(evaluate(&[
            Value::Text("café".into()), Value::Text("X".into()), Value::Integer(4),
        ]).unwrap(), Value::Text("cafX".into()));
        assert_eq!(evaluate(&[
            Value::Text("abcdef".into()), Value::Text("XY".into()),
            Value::Integer(3), Value::Integer(3),
        ]).unwrap(), Value::Text("abXYf".into()));
        assert!(evaluate(&[
            Value::Text("abc".into()), Value::Text("z".into()), Value::Integer(0),
        ]).is_err());
        assert_eq!(evaluate(&[
            Value::Text("Spark SQL".into()), Value::Text("CORE".into()),
            Value::Integer(7), Value::Integer(-1),
        ]).unwrap(), Value::Text("Spark CORE".into()));
        assert_eq!(evaluate(&[
            Value::Text("aéZ".into()), Value::Text("🙂!".into()),
            Value::Integer(2), Value::Integer(-1),
        ]).unwrap(), Value::Text("a🙂!".into()));
        assert_eq!(evaluate(&[
            Value::Text("abc".into()), Value::Text("X".into()),
            Value::Integer(2), Value::Integer(0),
        ]).unwrap(), Value::Text("aXbc".into()));
        assert_eq!(evaluate(&[
            Value::Text("a🙂b".into()), Value::Text("!".into()),
            Value::Integer(i64::MAX),
        ]).unwrap(), Value::Text("a🙂b!".into()));
        assert_eq!(evaluate(&[
            Value::Text("a🙂b".into()), Value::Text("!".into()),
            Value::Integer(2), Value::Integer(i64::MAX),
        ]).unwrap(), Value::Text("a!".into()));
    }
}

#[cfg(test)]
mod overlay_utf8_slice_perf_tests {
    use super::*;

    #[test]
    fn slices_unicode_boundaries_and_keeps_out_of_range_appends() {
        assert_eq!(evaluate(&[
            Value::Text("A🙂éZ".into()), Value::Text("!".into()),
            Value::Integer(2), Value::Integer(2),
        ]).unwrap(), Value::Text("A!Z".into()));
        assert_eq!(evaluate(&[
            Value::Text("abc".into()), Value::Text("!".into()), Value::Integer(99),
        ]).unwrap(), Value::Text("abc!".into()));
        assert_eq!(evaluate(&[
            Value::Text("é".into()), Value::Text("X".into()),
            Value::Integer(1), Value::Integer(0),
        ]).unwrap(), Value::Text("Xé".into()));
    }
}

#[cfg(test)]
mod common_suffix_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("common_suffix"));
        assert_eq!(common_suffix(&[Value::Text("one/東京".into()), Value::Text("two/東京".into())]).unwrap(), Value::Text("/東京".into()));
        assert_eq!(common_suffix(&[Value::Text("abc".into()), Value::Text("abc".into())]).unwrap(), Value::Text("abc".into()));
        assert_eq!(common_suffix(&[Value::Text("abc".into()), Value::Text("xyz".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(common_suffix(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod overlay_actual_output_budget_tests {
    use super::*;
    #[test]
    fn permits_large_replacements_that_shrink_within_budget() {
        let source = "a".repeat(9 * 1024 * 1024);
        let replacement = "b".repeat(8 * 1024 * 1024);
        let result = evaluate(&[Value::Text(source), Value::Text(replacement.clone()),
            Value::Integer(1), Value::Integer(9 * 1024 * 1024)]).unwrap();
        assert_eq!(result, Value::Text(replacement));
        assert!(evaluate(&[Value::Text("a".repeat(16 * 1024 * 1024)),
            Value::Text("b".into()), Value::Integer(1), Value::Integer(0)]).is_err());
    }
}

#[cfg(test)]
mod overlay_ascii_offset_perf_tests {
    use super::*;

    #[test]
    fn large_ascii_offsets_retain_exact_slice_boundaries() {
        let input = "a".repeat(1_000_000);
        let mut expected = input.clone();
        expected.replace_range(800_000..800_001, "XYZ");
        assert_eq!(evaluate(&[
            Value::Text(input), Value::Text("XYZ".into()),
            Value::Integer(800_001), Value::Integer(1),
        ]).unwrap(), Value::Text(expected));
    }

    #[test]
    fn unicode_offsets_and_suffix_identity_stay_correct() {
        assert_eq!(evaluate(&[
            Value::Text("é🙂abc".into()), Value::Text("界".into()),
            Value::Integer(2), Value::Integer(2),
        ]).unwrap(), Value::Text("é界bc".into()));
        assert_eq!(common_suffix(&[
            Value::Text("abc🙂é".into()), Value::Text("xyz🙂é".into()),
        ]).unwrap(), Value::Text("🙂é".into()));
        let same = Value::Text("é🙂".repeat(30_000));
        assert_eq!(common_suffix(&[same.clone(), same.clone()]).unwrap(), same);
    }
}
