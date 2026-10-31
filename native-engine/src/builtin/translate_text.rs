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
    if text.is_empty() {
        // The character mapping cardinality limit applies even when no output
        // characters are produced. Skip only the actual replacement work.
        if !from.is_ascii() &&
            from.chars().collect::<std::collections::HashSet<_>>().len() > 4096 {
            return Err(Error::Execution("TRANSLATE map exceeds 4096 characters".into()));
        }
        return Ok(Value::Text(String::new()));
    }
    if from.is_empty() { return Ok(Value::Text(text.to_owned())); }
    // Identical source and replacement positions cannot change output.
    // Preserve the Unicode mapping cardinality budget for oversized patterns.
    if from == to && (from.is_ascii() || from.chars().count() <= 4096) {
        return Ok(Value::Text(text.to_owned()));
    }
    if text.is_ascii() && from.is_ascii() {
        // ASCII source columns can use a small direct lookup even when
        // replacement scalars are multi-byte Unicode characters.
        // The outer Option distinguishes an unmapped byte from deletion.
        let mut mapping: [Option<Option<char>>; 128] = [None; 128];
        let mut replacements = to.chars();
        for byte in from.bytes() {
            let replacement = replacements.next();
            mapping[usize::from(byte)].get_or_insert(replacement);
        }
        let mut output = String::with_capacity(text.len());
        for byte in text.bytes() {
            match mapping[usize::from(byte)] {
                Some(Some(replacement)) => output.push(replacement),
                Some(None) => {}
                None => output.push(char::from(byte)),
            }
            if output.len() > 16 * 1024 * 1024 {
                return Err(Error::Execution("TRANSLATE output exceeds 16 MiB".into()));
            }
        }
        return Ok(Value::Text(output));
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

#[cfg(test)]
mod identity_translate_fast_path_tests {
    use super::*;
    #[test]
    fn leaves_text_unchanged_for_identity_mappings() {
        let input = Value::Text("café🙂東京abc".into());
        assert_eq!(evaluate(&[input.clone(), Value::Text("é🙂東京".into()),
            Value::Text("é🙂東京".into())]).unwrap(), input);
        assert_eq!(evaluate(&[Value::Text("aaa".into()), Value::Text("aa".into()),
            Value::Text("aa".into())]).unwrap(), Value::Text("aaa".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Text("x".into()),
            Value::Text("x".into())]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod translate_empty_text_tests {
    use super::*;

    #[test]
    fn empty_input_avoids_building_a_large_translation_map() {
        let from = Value::Text("αβγδεζηθικλμνξοπρστυφχψω".repeat(100));
        assert_eq!(evaluate(&[Value::Text(String::new()), from.clone(),
            Value::Text("!".into())]).unwrap(), Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Text(String::new()), from,
            Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text(String::new()),
            Value::Text("x".repeat(16_385)), Value::Text(String::new())]).is_err());
        let distinct = (0x1000..=0x2000)
            .map(|code| char::from_u32(code).unwrap()).collect::<String>();
        assert!(evaluate(&[Value::Text(String::new()), Value::Text(distinct),
            Value::Text("x".into())]).is_err());
    }
}

#[cfg(test)]
mod direct_lookup_unicode_tests {
    use super::*;

    fn translate(text: &str, from: &str, to: &str) -> Result<Value> {
        evaluate(&[
            Value::Text(text.into()),
            Value::Text(from.into()),
            Value::Text(to.into()),
        ])
    }

    #[test]
    fn ascii_source_maps_to_multibyte_unicode_without_losing_duplicate_rules() {
        assert_eq!(translate("ababa", "ab", "🙂é").unwrap(),
            Value::Text("🙂é🙂é🙂".into()));
        assert_eq!(translate("aabb", "aab", "X🙂Q").unwrap(),
            Value::Text("XXQQ".into()));
        assert_eq!(translate("abba", "ab", "é").unwrap(),
            Value::Text("éé".into()));
        assert_eq!(translate("abba", "ab", "").unwrap(),
            Value::Text("".into()));
        assert_eq!(translate("café", "a", "🙂").unwrap(),
            Value::Text("c🙂fé".into()));
    }

    #[test]
    fn expanded_ascii_input_still_respects_output_budget() {
        let input = "a".repeat(5 * 1024 * 1024);
        assert!(translate(&input, "a", "🙂").unwrap_err()
            .to_string().contains("16 MiB"));
    }
}
