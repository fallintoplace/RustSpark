use std::collections::HashMap;
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    let Some(from) = super::string(&args[1])? else { return Ok(Value::Null); };
    let Some(to) = super::string(&args[2])? else { return Ok(Value::Null); };
    if text.len() > 16 * 1024 * 1024 || from.len() > 16_384 || to.len() > 16_384 {
        return Err(Error::Execution("TRANSLATE input exceeds memory budget".into()));
    }
    if from.is_empty() { return Ok(Value::Text(text.to_owned())); }
    if text.is_ascii() && from.is_ascii() && to.is_ascii() {
        const UNMAPPED: i16 = -1;
        const REMOVE: i16 = -2;
        let mut mapping = [UNMAPPED; 128];
        for (index, &byte) in from.as_bytes().iter().enumerate() {
            if mapping[usize::from(byte)] == UNMAPPED {
                mapping[usize::from(byte)] = to.as_bytes().get(index)
                    .map_or(REMOVE, |replacement| i16::from(*replacement));
            }
        }
        let mut output = Vec::with_capacity(text.len());
        for &byte in text.as_bytes() {
            match mapping[usize::from(byte)] {
                UNMAPPED => output.push(byte),
                REMOVE => {},
                replacement => output.push(replacement as u8),
            }
        }
        return Ok(Value::Text(String::from_utf8(output)
            .expect("ASCII translations remain UTF-8")));
    }
    let mut mapping: HashMap<char, Option<char>> = HashMap::new();
    let mut replacement = to.chars();
    for character in from.chars() {
        let replacement_char = replacement.next();
        mapping.entry(character).or_insert(replacement_char);
        if mapping.len() > 4096 {
            return Err(Error::Execution("TRANSLATE map exceeds 4096 characters".into()));
        }
    }
    // Most mappings keep byte width unchanged; reserve source length up front.
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match mapping.get(&character) {
            Some(Some(replacement)) => output.push(*replacement),
            Some(None) => {},
            None => output.push(character),
        }
        if output.len() > 16 * 1024 * 1024 {
            return Err(Error::Execution("TRANSLATE output exceeds 16 MiB".into()));
        }
    }
    Ok(Value::Text(output))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "translate", min_args: 3, max_args: 3,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maps_unicode_characters_and_deletes_unpaired_source_characters() {
        assert_eq!(evaluate(&[
            Value::Text("café".into()), Value::Text("é".into()),
            Value::Text("e".into()),
        ]).unwrap(), Value::Text("cafe".into()));
        assert_eq!(evaluate(&[
            Value::Text("abc".into()), Value::Text("bc".into()),
            Value::Text("X".into()),
        ]).unwrap(), Value::Text("aX".into()));
        assert_eq!(evaluate(&[
            Value::Text("aba".repeat(1000)), Value::Text("ab".into()),
            Value::Text("bc".into()),
        ]).unwrap(), Value::Text("bcb".repeat(1000)));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text("a".into()), Value::Text("b".into()),
        ]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod empty_translation_perf_tests {
    use super::*;
    #[test]
    fn empty_source_keeps_unicode_and_null_semantics() {
        assert_eq!(evaluate(&[
            Value::Text("é🙂東京".into()), Value::Text("".into()), Value::Text("replacement".into())
        ]).unwrap(), Value::Text("é🙂東京".into()));
        assert_eq!(evaluate(&[
            Value::Null, Value::Text("".into()), Value::Text("".into())
        ]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_translate_fast_path_tests {
    use super::*;
    #[test]
    fn preserves_first_mapping_and_deletions() {
        let translated = |text: &str, from: &str, to: &str| evaluate(&[
            Value::Text(text.into()), Value::Text(from.into()), Value::Text(to.into())
        ]).unwrap();
        assert_eq!(translated("abca", "aac", "XY"), Value::Text("XbX".into()));
        assert_eq!(translated("cab", "ab", "Z"), Value::Text("cZ".into()));
        assert_eq!(translated("café", "é", "e"), Value::Text("cafe".into()));
        assert_eq!(translated("abc", "", "x"), Value::Text("abc".into()));
    }
}
