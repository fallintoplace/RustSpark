use std::collections::HashSet;
use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn trim(args: &[Value], left: bool, right: bool) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let characters = if args.len() == 2 {
        let Some(characters) = super::string(&args[1])? else { return Ok(Value::Null); };
        Some(characters)
    } else { None };
    let ascii_set = characters
        .filter(|chars| chars.len() >= 8 && chars.is_ascii())
        .map(|chars| {
            let mut lookup = [false; 128];
            for byte in chars.bytes() { lookup[byte as usize] = true; }
            lookup
        });
    let unicode_set = characters
        .filter(|chars| chars.len() >= 32 && !chars.is_ascii())
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
    if input.is_ascii() {
        let mut output = String::with_capacity(input.len());
        let mut pending = false;
        for byte in input.bytes() {
            if byte.is_ascii_whitespace() || byte == 0x0b {
                pending = !output.is_empty();
            } else {
                if pending { output.push(' '); }
                pending = false;
                output.push(char::from(byte));
            }
        }
        return Ok(Value::Text(output));
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
mod normalize_space_ascii_fast_path_tests {
    use super::*;
    #[test]
    fn collapses_ascii_and_preserves_multibyte_whitespace_semantics() {
        assert_eq!(normalize_space(&[Value::Text("\t A  B \r\n C  ".into())]).unwrap(), Value::Text("A B C".into()));
        assert_eq!(normalize_space(&[Value::Text(" A\u{00A0}B ".into())]).unwrap(), Value::Text("A B".into()));
        assert_eq!(normalize_space(&[Value::Text("   ".into())]).unwrap(), Value::Text(String::new()));
        assert_eq!(normalize_space(&[Value::Text("A\u{000B}B".into())]).unwrap(), Value::Text("A B".into()));
    }
}
