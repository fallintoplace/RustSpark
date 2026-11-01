use std::collections::HashSet;
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn trim(args: &[Value], left: bool, right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let characters = if args.len() == 2 {
        let Some(characters) = super::string(&args[1])? else { return Ok(Value::Null); };
        Some(characters)
    } else { None };
    // Empty input needs no character-set lookup, but validate the optional argument first.
    if text.is_empty() { return Ok(Value::Text(String::new())); }
    // Explicit empty trim sets cannot match any character.
    if characters == Some("") {
        return Ok(Value::Text(text.to_owned()));
    }
    // An ASCII input cannot match a non-ASCII trim character. Even with
    // mixed Unicode trim sets, use an ASCII lookup instead of building a
    // per-row HashSet of scalars that could never match this input.
    let ascii_set = characters
        .filter(|chars| chars.len() >= 8 && (chars.is_ascii() || text.is_ascii()))
        .map(|chars| {
            let mut lookup = [false; 128];
            for character in chars.chars() {
                if character.is_ascii() { lookup[character as usize] = true; }
            }
            lookup
        });
    let unicode_set = characters
        .filter(|chars| chars.len() >= 32 && !chars.is_ascii() && !text.is_ascii())
        .map(|chars| chars.chars().collect::<HashSet<_>>());
    let contains = |ch: char| {
        if let Some(lookup) = &ascii_set {
            ch.is_ascii() && lookup[ch as usize]
        } else if let Some(lookup) = &unicode_set {
            lookup.contains(&ch)
        } else {
            characters.is_some_and(|chars| chars.contains(ch))
        }
    };
    let value = match characters {
        Some(_) if left && right =>
            text.trim_matches(contains),
        Some(_) if left =>
            text.trim_start_matches(contains),
        Some(_) if right =>
            text.trim_end_matches(contains),
        None if left && right => text.trim_matches(' '),
        None if left => text.trim_start_matches(' '),
        None if right => text.trim_end_matches(' '),
        _ => text,
    };
    if value.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("TRIM result exceeds native 16 MiB budget".into()));
    }
    Ok(Value::Text(value.into()))
}

fn btrim(args: &[Value]) -> Result<Value> { trim(args, true, true) }
fn ltrim(args: &[Value]) -> Result<Value> { trim(args, true, false) }
fn rtrim(args: &[Value]) -> Result<Value> { trim(args, false, true) }

fn normalize_space_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}
fn normalize_space(args: &[Value]) -> Result<Value> {
    let Some(input) = super::string(&args[0])? else { return Ok(Value::Null); };
    if input.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("NORMALIZE_SPACE input exceeds native 16 MiB budget".into()));
    }
    if input.is_ascii() {
        let mut output = Vec::with_capacity(input.len());
        let mut pending = false;
        for &byte in input.as_bytes() {
            if byte.is_ascii_whitespace() || byte == b'\x0b' {
                pending = !output.is_empty();
            } else {
                if pending { output.push(b' '); }
                pending = false;
                output.push(byte);
            }
        }
        return Ok(Value::Text(String::from_utf8(output)
            .expect("normalizing ASCII bytes preserves UTF-8")));
    }
    let mut output = String::with_capacity(input.len());
    let mut pending = false;
    for character in input.chars() {
        if character.is_whitespace() {
            pending = !output.is_empty();
        } else {
            if pending { output.push(' '); }
            pending = false;
            output.push(character);
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "normalize_space", min_args: 1, max_args: 1, result_type: normalize_space_type, call: normalize_space },
    Function { name: "btrim", min_args: 1, max_args: 2, result_type: output, call: btrim },
    Function { name: "ltrim", min_args: 1, max_args: 2, result_type: output, call: ltrim },
    Function { name: "rtrim", min_args: 1, max_args: 2, result_type: output, call: rtrim },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_character_sets_and_whitespace() {
        assert_eq!(btrim(&[Value::Text("ééhelloé".into()),
            Value::Text("é".into())]).unwrap(), Value::Text("hello".into()));
        assert_eq!(ltrim(&[Value::Text("  x ".into())]).unwrap(), Value::Text("x ".into()));
        assert_eq!(rtrim(&[Value::Text("  x ".into())]).unwrap(), Value::Text("  x".into()));
        assert_eq!(btrim(&[Value::Text(" \t x\n ".into())]).unwrap(), Value::Text("\t x\n".into()));
        assert_eq!(ltrim(&[Value::Text("\t  x".into())]).unwrap(), Value::Text("\t  x".into()));
        assert_eq!(rtrim(&[Value::Text("x  \n".into())]).unwrap(), Value::Text("x  \n".into()));
    }
}

#[cfg(test)]
mod ascii_trim_lookup_perf_tests {
    use super::*;

    #[test]
    fn wide_ascii_sets_keep_unicode_and_trim_boundaries() {
        let chars = Value::Text("0123456789abcdef".into());
        assert_eq!(btrim(&[
            Value::Text("a0🙂middle🙂0f".into()), chars.clone(),
        ]).unwrap(), Value::Text("🙂middle🙂".into()));
        assert_eq!(ltrim(&[
            Value::Text("ff🙂ff".into()), chars,
        ]).unwrap(), Value::Text("🙂ff".into()));
        assert_eq!(btrim(&[
            Value::Text("ééhelloé".into()), Value::Text("é".into()),
        ]).unwrap(), Value::Text("hello".into()));
    }
}

#[cfg(test)]
mod unicode_trim_hash_set_tests {
    use super::*;
    #[test]
    fn trims_wide_unicode_sets_without_touching_middle() {
        let chars = Value::Text("αβγδεζηθικλμνξοπρστυφχψω🙂".into());
        assert_eq!(btrim(&[Value::Text("ωαhelloβ🙂".into()), chars]).unwrap(),
            Value::Text("hello".into()));
    }
}

#[cfg(test)]
mod normalize_space_extension_tests {
    use super::*;
    #[test]
    fn covers_null_and_boundary_values() {
        assert!(crate::builtin::supports("normalize_space"));
        assert_eq!(normalize_space(&[Value::Text(" \t A\u{00A0} B \n".into())]).unwrap(), Value::Text("A B".into()));
        assert_eq!(normalize_space(&[Value::Text(" \t ".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(normalize_space(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod empty_trim_character_set_tests {
    use super::*;
    #[test]
    fn empty_set_preserves_whitespace_and_unicode() {
        let text = Value::Text("  é🙂  ".into());
        for trim in [btrim as fn(&[Value]) -> Result<Value>, ltrim, rtrim] {
            assert_eq!(trim(&[text.clone(), Value::Text(String::new())]).unwrap(), text);
        }
        assert_eq!(btrim(&[Value::Text("  a  ".into())]).unwrap(), Value::Text("a".into()));
        assert_eq!(btrim(&[Value::Null, Value::Text(String::new())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod normalize_ascii_whitespace_tests {
    use super::*;
    #[test]
    fn collapses_many_ascii_fields_without_changing_unicode() {
        let text = " a\t\t b\n c  ".repeat(2000);
        let actual = normalize_space(&[Value::Text(text)]).unwrap();
        let Value::Text(text) = actual else { panic!("text"); };
        assert_eq!(text.split_whitespace().count(), 6000);
        assert!(text.starts_with("a b c a b c"));
        assert_eq!(normalize_space(&[Value::Text("a\u{00a0} b".into())]).unwrap(), Value::Text("a b".into()));
    }
}

#[cfg(test)]
mod trim_empty_text_fast_path_tests {
    use super::*;

    #[test]
    fn empty_text_skips_large_character_lookup_and_keeps_nulls() {
        let chars = Value::Text("αβγδεζηθικλμνξοπρστυφχψω".repeat(2000));
        assert_eq!(btrim(&[Value::Text(String::new()), chars.clone()]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(ltrim(&[Value::Text(String::new()), chars.clone()]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(rtrim(&[Value::Text(String::new()), chars]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(btrim(&[Value::Text(String::new()), Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod mixed_unicode_trim_on_ascii_tests {
    use super::*;

    fn apply(text: &str, chars: &str, left: bool, right: bool) -> Value {
        trim(&[
            Value::Text(text.into()), Value::Text(chars.into())
        ], left, right).unwrap()
    }

    #[test]
    fn ascii_inputs_use_only_the_relevant_part_of_mixed_unicode_sets() {
        let trim_set = format!("{}abcXYZ", "🙂é界".repeat(64));
        assert_eq!(apply("aXahelloYc", &trim_set, true, true),
            Value::Text("hello".into()));
        assert_eq!(apply("aXahelloYc", &trim_set, true, false),
            Value::Text("helloYc".into()));
        assert_eq!(apply("aXahelloYc", &trim_set, false, true),
            Value::Text("aXahello".into()));
        assert_eq!(apply("helloworld", &trim_set, true, true),
            Value::Text("helloworld".into()));
    }

    #[test]
    fn nonascii_inputs_still_consult_unicode_trim_membership() {
        let trim_set = format!("{}a", "🙂é界".repeat(32));
        assert_eq!(apply("🙂éahello界🙂", &trim_set, true, true),
            Value::Text("hello".into()));
        assert_eq!(apply("🙂éahello界🙂", &trim_set, true, false),
            Value::Text("hello界🙂".into()));
    }
}

#[cfg(test)]
mod ascii_normalize_byte_output_regressions {
    use super::*;

    fn reference(text: &str) -> String {
        text.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn all_ascii_whitespace_and_long_fields_match_reference() {
        let long = "a b c".repeat(100_000);
        for text in [
            "", "  ", "abc", " a   b  ", "\t\r\n a\u{000B}b\u{000C}  c \t",
            "\0  a \0", long.as_str(),
        ] {
            let actual = normalize_space(&[Value::Text(text.into())]).unwrap();
            assert_eq!(actual, Value::Text(reference(text)));
        }
        assert_eq!(normalize_space(&[Value::Text("é\u{00A0}界".into())]).unwrap(),
            Value::Text("é 界".into()));
        assert_eq!(normalize_space(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bounded_trim_normalize_tests {
    use super::*;
    #[test]
    fn permits_short_trimmed_results_but_rejects_large_retained_text() {
        let huge = "a".repeat(16 * 1024 * 1024 + 1);
        assert!(matches!(btrim(&[Value::Text(huge.clone())]),
            Err(Error::Execution(_))));
        let padded = format!("{}ok{}", " ".repeat(8 * 1024 * 1024), " ".repeat(8 * 1024 * 1024));
        assert_eq!(btrim(&[Value::Text(padded)]).unwrap(), Value::Text("ok".into()));
        assert_eq!(btrim(&[Value::Null]).unwrap(), Value::Null);
    }
    #[test]
    fn limits_whitespace_normalization_before_large_allocations() {
        assert!(matches!(normalize_space(&[Value::Text("a".repeat(16 * 1024 * 1024 + 1))]),
            Err(Error::Execution(_))));
        assert_eq!(normalize_space(&[Value::Text(" a\t b ".into())]).unwrap(),
            Value::Text("a b".into()));
        assert_eq!(normalize_space(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod vertical_tab_normalization_tests {
    use super::*;

    #[test]
    fn ascii_and_unicode_paths_agree_with_split_whitespace() {
        for text in [
            "", "a\x0bb", "\x0b\t a\x0b b \x0b",
            "a\x0b\x0bb", "a\x0cb", "éa\x0bb",
            "é\x0b🙂\x0b東京", "\x0b", "a\r\nb",
        ] {
            let expected = text.split_whitespace().collect::<Vec<_>>().join(" ");
            assert_eq!(normalize_space(&[Value::Text(text.into())]).unwrap(),
                Value::Text(expected), "input={text:?}");
        }
        assert_eq!(normalize_space(&[Value::Null]).unwrap(), Value::Null);
    }
}
