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
    // Unicode input often uses an ASCII replacement alphabet. Avoid a
    // per-row HashMap when all source and target scalars fit a fixed table.
    // Non-ASCII input scalars cannot match this mapping and pass through.
    if from.is_ascii() && to.is_ascii() {
        const UNMAPPED: i16 = -1;
        const REMOVE: i16 = -2;
        let mut mapping = [UNMAPPED; 128];
        for (index, &byte) in from.as_bytes().iter().enumerate() {
            if mapping[usize::from(byte)] == UNMAPPED {
                mapping[usize::from(byte)] = to.as_bytes().get(index)
                    .map_or(REMOVE, |replacement| i16::from(*replacement));
            }
        }
        let mut output = String::with_capacity(text.len());
        for character in text.chars() {
            if character.is_ascii() {
                match mapping[character as usize] {
                    UNMAPPED => output.push(character),
                    REMOVE => {},
                    replacement => output.push(char::from(replacement as u8)),
                }
            } else {
                output.push(character);
            }
        }
        return Ok(Value::Text(output));
    }
    if text.is_ascii() {
        // Mixed Unicode source maps still need their distinct-character
        // limit, even though non-ASCII keys cannot match ASCII input.
        if !from.is_ascii() &&
            from.chars().collect::<std::collections::HashSet<_>>().len() > 4096 {
            return Err(Error::Execution("TRANSLATE map exceeds 4096 characters".into()));
        }
        // Keep ASCII lookup speed even when source or replacement
        // scalars are Unicode.
        // Some(None) denotes deletion; None means the source byte is unmapped.
        let mut mapping: [Option<Option<char>>; 128] = [None; 128];
        let mut replacements = to.chars();
        for character in from.chars() {
            // Consume replacement positions for every source scalar,
            // including ones that cannot occur in an ASCII input.
            let replacement = replacements.next();
            if character.is_ascii() && mapping[character as usize].is_none() {
                mapping[character as usize] = Some(replacement);
            }
        }
        let mut output = String::with_capacity(text.len());
        for &byte in text.as_bytes() {
            let character = match mapping[usize::from(byte)] {
                None => Some(char::from(byte)),
                Some(mapped) => mapped,
            };
            if let Some(character) = character {
                if character.len_utf8() > (16 * 1024 * 1024_usize).saturating_sub(output.len()) {
                    return Err(Error::Execution("TRANSLATE output exceeds 16 MiB".into()));
                }
                output.push(character);
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
mod ascii_lookup_unicode_translation_regressions {
    use super::*;

    #[test]
    fn preserves_first_mapping_deletion_and_utf8_growth_limits() {
        let run = |source: &str, from: &str, to: &str| evaluate(&[
            Value::Text(source.into()), Value::Text(from.into()),
            Value::Text(to.into()),
        ]).unwrap();
        assert_eq!(run("aba", "aab", "🙂éZ"), Value::Text("🙂Z🙂".into()));
        assert_eq!(run("abcXYZ", "abc", "界"), Value::Text("界XYZ".into()));
        assert_eq!(run("abba", "ab", "🙂"), Value::Text("🙂🙂".into()));
        assert_eq!(run("abc", "", "🙂"), Value::Text("abc".into()));
        assert_eq!(run("éabc", "a", "🙂"), Value::Text("é🙂bc".into()));
        assert!(matches!(evaluate(&[
            Value::Text("a".repeat(5 * 1024 * 1024)),
            Value::Text("a".into()), Value::Text("🙂".into()),
        ]), Err(Error::Execution(_))));
    }
}

#[cfg(test)]
mod mixed_unicode_ascii_translation_regressions {
    use super::*;

    #[test]
    fn preserves_unicode_mapping_positions_duplicates_and_deletions() {
        let translate = |text: &str, from: &str, to: &str| {
            evaluate(&[
                Value::Text(text.into()),
                Value::Text(from.into()),
                Value::Text(to.into()),
            ]).unwrap()
        };
        assert_eq!(translate("a1a", "🙂aé", "XYQ"), Value::Text("Y1Y".into()));
        assert_eq!(translate("aba", "éab", "🙂Z"), Value::Text("ZZ".into()));
        assert_eq!(translate("aba", "🙂aa", "XYZ"), Value::Text("YbY".into()));
        assert_eq!(translate("ba", "éb", "🙂"), Value::Text("a".into()));
        assert_eq!(translate("foo", "é🙂", "ab"), Value::Text("foo".into()));
        assert_eq!(translate("", "", ""), Value::Text(String::new()));
    }
}

#[cfg(test)]
mod unicode_text_ascii_mapping_regressions {
    use super::*;

    #[test]
    fn replaces_ascii_scalars_inside_unicode_text_without_changing_other_scalars() {
        let translate = |input: &str, from: &str, to: &str| evaluate(&[
            Value::Text(input.into()), Value::Text(from.into()), Value::Text(to.into()),
        ]).unwrap();
        assert_eq!(translate("éa🙂bab東京", "ab", "XY"), Value::Text("éX🙂YXY東京".into()));
        assert_eq!(translate("éa🙂bab東京", "ab", "Z"), Value::Text("éZ🙂ZZ東京".into()));
        assert_eq!(translate("éa🙂bab東京", "aab", "XYZ"), Value::Text("éX🙂ZXZ東京".into()));
        assert_eq!(translate("é🙂東京", "ab", "XY"), Value::Text("é🙂東京".into()));
    }

    #[test]
    fn large_unicode_payload_keeps_byte_size_and_duplicate_mapping_rules() {
        let text = format!("{}a🙂b", "東京é".repeat(10_000));
        let expected = format!("{}X🙂Y", "東京é".repeat(10_000));
        assert_eq!(evaluate(&[Value::Text(text), Value::Text("ab".into()),
            Value::Text("XY".into())]).unwrap(), Value::Text(expected));
        assert_eq!(evaluate(&[Value::Null, Value::Text("ab".into()),
            Value::Text("XY".into())]).unwrap(), Value::Null);
    }
}
