use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text && matches!(types[1], DataType::Integer | DataType::Text) {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("LEFT/RIGHT require text and integer or text width".into()))
    }
}

fn slice(args: &[Value], from_right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let width = match &args[1] {
        Value::Integer(width) => *width,
        Value::Text(text) => match text.trim().parse::<i64>() {
            Ok(width) => width,
            Err(_) => return Ok(Value::Null),
        },
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("LEFT/RIGHT width must be integer or numeric text".into())),
    };
    // Spark returns an empty string when length is zero or negative.
    let count = usize::try_from(width.max(0)).unwrap_or(usize::MAX);
    let result = if count == 0 {
        ""
    } else if count >= text.len() {
        // There cannot be more Unicode characters than UTF-8 bytes.
        text
    } else if text.is_ascii() {
        if from_right {
            &text[text.len().saturating_sub(count)..]
        } else {
            &text[..count.min(text.len())]
        }
    } else if from_right {
        let start = text.char_indices().rev().nth(count - 1)
            .map_or(0, |(byte, _)| byte);
        &text[start..]
    } else {
        let end = text.char_indices().nth(count)
            .map_or(text.len(), |(byte, _)| byte);
        &text[..end]
    };
    Ok(Value::Text(result.to_owned()))
}

fn left(args: &[Value]) -> Result<Value> { slice(args, false) }
fn right(args: &[Value]) -> Result<Value> { slice(args, true) }

fn reverse_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn reverse(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("REVERSE input exceeds 16 MiB".into()));
    }
    if text.is_ascii() {
        let mut bytes = text.as_bytes().to_vec();
        bytes.reverse();
        return Ok(Value::Text(String::from_utf8(bytes)
            .expect("reversing ASCII bytes preserves UTF-8")));
    }
    let mut result = String::with_capacity(text.len());
    for character in text.chars().rev() {
        result.push(character);
    }
    Ok(Value::Text(result))
}

fn common_prefix_type(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn common_prefix(args: &[Value]) -> Result<Value> {
    let Some(left) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::string(&args[1])? else { return Ok(Value::Null); };
    if left == right {
        // Compare bytes once instead of decoding every matching Unicode scalar.
        return Ok(Value::Text(left.to_owned()));
    }
    let mut end = 0;
    for (&first, &second) in left.as_bytes().iter().zip(right.as_bytes()) {
        if first != second { break; }
        end += 1;
    }
    // Different Unicode scalars may share leading UTF-8 bytes. Exclude any
    // partial scalar from the returned prefix.
    while !left.is_char_boundary(end) { end -= 1; }
    Ok(Value::Text(left[..end].to_string()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "common_prefix", min_args: 2, max_args: 2, result_type: common_prefix_type, call: common_prefix },
    Function { name: "left", min_args: 2, max_args: 2, result_type: output, call: left },
    Function { name: "right", min_args: 2, max_args: 2, result_type: output, call: right },
    Function { name: "reverse", min_args: 1, max_args: 1, result_type: reverse_type, call: reverse },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slices_on_unicode_character_boundaries() {
        let input = Value::Text("café".into());
        assert_eq!(left(&[input.clone(), Value::Integer(3)]).unwrap(),
            Value::Text("caf".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(2)]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(0)]).unwrap(),
            Value::Text("".into()));
        assert_eq!(left(&[input.clone(), Value::Integer(-1)]).unwrap(), Value::Text("".into()));
        assert_eq!(right(&[input, Value::Integer(-3)]).unwrap(), Value::Text("".into()));
        assert_eq!(reverse(&[Value::Text("hé🙂".into())]).unwrap(),
            Value::Text("🙂éh".into()));
        assert_eq!(reverse(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(reverse(&[Value::Null]).unwrap(), Value::Null);
        assert!(reverse_type(&[DataType::Integer]).is_err());
        assert!(crate::builtin::supports("REVERSE"));
    }
}

#[cfg(test)]
mod utf8_left_right_perf_tests {
    use super::*;

    #[test]
    fn returns_exact_utf8_boundaries_for_short_and_large_widths() {
        let input = Value::Text("a🙂東京é".into());
        assert_eq!(left(&[input.clone(), Value::Integer(3)]).unwrap(),
            Value::Text("a🙂東".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(2)]).unwrap(),
            Value::Text("京é".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(9999)]).unwrap(), input.clone());
        assert_eq!(left(&[input.clone(), Value::Integer(i64::MAX)]).unwrap(), input.clone());
        assert_eq!(right(&[input.clone(), Value::Integer(99)]).unwrap(), input);
        assert_eq!(left(&[Value::Text("abc".into()), Value::Integer(0)]).unwrap(),
            Value::Text("".into()));
    }
}

#[cfg(test)]
mod spark_string_width_tests {
    use super::*;

    #[test]
    fn numeric_string_widths_work_with_unicode_and_invalid_widths_propagate_null() {
        let input = Value::Text("café".into());
        assert_eq!(left(&[input.clone(), Value::Text(" 3 ".into())]).unwrap(),
            Value::Text("caf".into()));
        assert_eq!(right(&[input.clone(), Value::Text("2".into())]).unwrap(),
            Value::Text("fé".into()));
        assert_eq!(right(&[input.clone(), Value::Text("-2".into())]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(left(&[input.clone(), Value::Text("n/a".into())]).unwrap(), Value::Null);
        assert_eq!(left(&[input, Value::Null]).unwrap(), Value::Null);
        assert_eq!(output(&[DataType::Text, DataType::Text]).unwrap(), DataType::Text);
    }
}

#[cfg(test)]
mod common_prefix_extension_tests {
    use super::*;
    #[test]
    fn preserves_unicode_and_null_semantics() {
        assert!(crate::builtin::supports("common_prefix"));
        assert_eq!(common_prefix(&[Value::Text("café-one".into()), Value::Text("café-two".into())]).unwrap(), Value::Text("café-".into()));
        assert_eq!(common_prefix(&[Value::Text("abc".into()), Value::Text("xyz".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(common_prefix(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_width_slice_tests {
    use super::*;
    #[test]
    fn slices_long_ascii_and_preserves_unicode_fallback() {
        let input = Value::Text("abcd".repeat(20_000));
        assert_eq!(left(&[input.clone(), Value::Integer(3)]).unwrap(), Value::Text("abc".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(3)]).unwrap(), Value::Text("bcd".into()));
        assert_eq!(right(&[input.clone(), Value::Integer(i64::MAX)]).unwrap(), input);
        assert_eq!(left(&[Value::Text("🙂abc".into()), Value::Integer(2)]).unwrap(),
            Value::Text("🙂a".into()));
    }
}

#[cfg(test)]
mod equal_common_prefix_fast_path_tests {
    use super::*;
    #[test]
    fn identical_unicode_strings_keep_their_full_prefix() {
        let text = Value::Text("🙂é東京".repeat(1000));
        assert_eq!(common_prefix(&[text.clone(), text.clone()]).unwrap(), text);
        assert_eq!(common_prefix(&[Value::Text("🙂a".into()),
            Value::Text("🙂b".into())]).unwrap(), Value::Text("🙂".into()));
        assert_eq!(common_prefix(&[Value::Text("".into()),
            Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(common_prefix(&[Value::Null, Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod byte_oriented_text_fast_path_tests {
    use super::*;

    #[test]
    fn common_prefix_never_returns_partial_unicode_characters() {
        for (left, right) in [
            ("éclair", "êclair"),
            ("🙂one", "🙃two"),
            ("é🙂alpha", "é🙂beta"),
            ("abc", "abcdef"),
            ("café-one", "café-two"),
            ("", "x"),
        ] {
            let expected = left.chars().zip(right.chars())
                .take_while(|(a, b)| a == b)
                .map(|(a, _)| a)
                .collect::<String>();
            assert_eq!(
                common_prefix(&[Value::Text(left.into()), Value::Text(right.into())]).unwrap(),
                Value::Text(expected)
            );
        }
        let shared = "x".repeat(100_000);
        assert_eq!(
            common_prefix(&[
                Value::Text(format!("{shared}é")),
                Value::Text(format!("{shared}ê")),
            ]).unwrap(),
            Value::Text(shared)
        );
    }

    #[test]
    fn ascii_reversal_keeps_unicode_fallback() {
        let ascii = "abc123XYZ".repeat(20_000);
        let expected = ascii.chars().rev().collect::<String>();
        assert_eq!(reverse(&[Value::Text(ascii)]).unwrap(),
            Value::Text(expected));
        assert_eq!(reverse(&[Value::Text("a🙂é".into())]).unwrap(),
            Value::Text("é🙂a".into()));
        assert_eq!(reverse(&[Value::Null]).unwrap(), Value::Null);
    }
}
