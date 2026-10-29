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
    if text.len().checked_add(replacement.len()).is_none_or(|n| n > 16 * 1024 * 1024) {
        return Err(Error::Execution("OVERLAY input exceeds native 16 MiB budget".into()));
    }
    let index = usize::try_from(start - 1).unwrap_or(usize::MAX);
    let begin = if text.is_ascii() { index.min(text.len()) } else {
        text.char_indices().nth(index).map_or(text.len(), |(byte, _)| byte)
    };
    let end = if text.is_ascii() { begin.saturating_add(count).min(text.len()) } else {
        text[begin..].char_indices().nth(count)
            .map_or(text.len(), |(byte, _)| begin + byte)
    };
    let mut result = String::with_capacity(text.len().saturating_add(replacement.len()));
    result.push_str(&text[..begin]);
    result.push_str(replacement);
    result.push_str(&text[end..]);
    Ok(Value::Text(result))
}


fn common_suffix_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn common_suffix(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
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
mod ascii_overlay_offset_tests {
    use super::*;
    #[test]
    fn fast_path_preserves_insert_replace_and_append() {
        let input = Value::Text("a".repeat(100_000));
        let result = evaluate(&[input.clone(), Value::Text("XY".into()), Value::Integer(50_001), Value::Integer(2)]).unwrap();
        let Value::Text(text) = result else { panic!("expected text"); };
        assert_eq!(&text[49_999..50_003], "aXYa");
        assert_eq!(text.len(), 100_000);
        assert_eq!(evaluate(&[Value::Text("abc".into()), Value::Text("!".into()), Value::Integer(10)]).unwrap(), Value::Text("abc!".into()));
    }
}
