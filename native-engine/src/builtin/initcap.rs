use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn push_checked(output: &mut String, character: char, limit: usize) -> Result<()> {
    let width = character.len_utf8();
    if width > limit.saturating_sub(output.len()) {
        return Err(Error::Execution("INITCAP output exceeds byte limit".into()));
    }
    if width > output.capacity().saturating_sub(output.len()) {
        output.try_reserve(width)
            .map_err(|_| Error::Execution("INITCAP output allocation failed".into()))?;
    }
    output.push(character);
    Ok(())
}

fn push_cased(output: &mut String, ch: char, capitalize: bool, limit: usize) -> Result<()> {
    if ch.is_ascii() {
        push_checked(output, if capitalize { ch.to_ascii_uppercase() }
            else { ch.to_ascii_lowercase() }, limit)?;
    } else if capitalize {
        for mapped in ch.to_uppercase() { push_checked(output, mapped, limit)?; }
    } else {
        for mapped in ch.to_lowercase() { push_checked(output, mapped, limit)?; }
    }
    Ok(())
}

fn initcap_bounded(text: &str, limit: usize) -> Result<String> {
    if text.len() > limit {
        return Err(Error::Execution("INITCAP input exceeds byte limit".into()));
    }
    if text.is_ascii() {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(text.len())
            .map_err(|_| Error::Execution("INITCAP output allocation failed".into()))?;
        bytes.extend_from_slice(text.as_bytes());
        let mut start_of_word = true;
        for byte in &mut bytes {
            // Keep ASCII boundaries equivalent to char::is_whitespace.
            if byte.is_ascii_whitespace() || *byte == b'\x0b' {
                start_of_word = true;
            } else {
                *byte = if start_of_word { byte.to_ascii_uppercase() }
                    else { byte.to_ascii_lowercase() };
                start_of_word = false;
            }
        }
        return Ok(String::from_utf8(bytes).expect("ASCII input"));
    }
    let mut result = String::new();
    result.try_reserve_exact(text.len())
        .map_err(|_| Error::Execution("INITCAP output allocation failed".into()))?;
    let mut start_of_word = true;
    for ch in text.chars() {
        if ch.is_whitespace() {
            push_checked(&mut result, ch, limit)?;
            start_of_word = true;
        } else {
            push_cased(&mut result, ch, start_of_word, limit)?;
            start_of_word = false;
        }
    }
    Ok(result)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Text(initcap_bounded(text, 16 * 1024 * 1024)?))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "initcap", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn title_cases_unicode_words_only_after_whitespace() {
        assert_eq!(evaluate(&[Value::Text("éCOLE   sQL-test".into())]).unwrap(),
            Value::Text("École   Sql-test".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod ascii_initcap_perf_tests {
    use super::*;
    #[test]
    fn ascii_fast_path_and_unicode_expansion_are_unchanged() {
        assert_eq!(evaluate(&[Value::Text("hello WORLD 2026\tRUSTSPARK".into())]).unwrap(),
            Value::Text("Hello World 2026\tRustspark".into()));
        assert_eq!(evaluate(&[Value::Text("straße ßET".into())]).unwrap(),
            Value::Text("Straße SSet".into()));
    }
}

#[cfg(test)]
mod initcap_ascii_bytes_tests {
    use super::*;
    #[test]
    fn retains_ascii_whitespace_boundaries() {
        assert_eq!(evaluate(&[Value::Text("aB\tcD\neF\x0bgH".into())]).unwrap(),
            Value::Text("Ab\tCd\nEf\x0bGh".into()));
        // Backslash followed by 't' or 'n' is literal text, not whitespace.
        assert_eq!(evaluate(&[Value::Text("aB\\tcD\\neF".into())]).unwrap(),
            Value::Text("Ab\\tcd\\nef".into()));
        assert_eq!(evaluate(&[Value::Text("éCOLE".into())]).unwrap(), Value::Text("École".into()));
    }
}

#[cfg(test)]
mod initcap_bounded_output_tests {
    use super::*;
    #[test]
    fn preserves_titlecase_and_rejects_excessive_input() {
        assert_eq!(initcap_bounded("éCOLE wow", 16).unwrap(), "École Wow");
        assert!(initcap_bounded("éCOLE wow", 4).is_err());
        assert_eq!(initcap_bounded("abc DEF", 7).unwrap(), "Abc Def");
    }
}

#[cfg(test)]
mod fallible_initcap_reservation_tests {
    use super::*;
    #[test]
    fn unicode_expansion_checks_byte_ceiling_before_append() {
        assert_eq!(initcap_bounded("straße ßET", 32).unwrap(), "Straße SSet");
        assert!(initcap_bounded("ΐ", 3).is_err());
        assert_eq!(initcap_bounded("a B", 3).unwrap(), "A B");
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod shared_initcap_unicode_mapping_tests {
    use super::*;

    #[test]
    fn expands_unicode_without_changing_word_boundaries() {
        assert_eq!(initcap_bounded("straße ßET", 32).unwrap(), "Straße SSet");
        assert_eq!(initcap_bounded("éCOLE\tABC", 32).unwrap(), "École\tAbc");
        assert!(matches!(initcap_bounded("ΐ", 3), Err(Error::Execution(_))));
        assert_eq!(initcap_bounded("A\nB", 3).unwrap(), "A\nB");
    }
}
