//! Shared bounded Unicode case mapping for the native text built-ins.
use crate::{Error, Result};

pub(super) fn convert_bounded<I>(
    text: &str, limit: usize, name: &str,
    ascii: impl Fn(&str) -> String,
    unicode: impl Fn(char) -> I,
) -> Result<String>
where
    I: Iterator<Item = char>,
{
    if text.len() > limit {
        return Err(Error::Execution(format!("{name} input exceeds byte limit")));
    }
    if text.is_ascii() {
        return Ok(ascii(text));
    }
    let mut result = String::new();
    result.try_reserve_exact(text.len())
        .map_err(|_| Error::Execution(format!("{name} output allocation failed")))?;
    for character in text.chars() {
        for mapped in unicode(character) {
            if mapped.len_utf8() > limit.saturating_sub(result.len()) {
                return Err(Error::Execution(format!("{name} output exceeds byte limit")));
            }
            if mapped.len_utf8() > result.capacity().saturating_sub(result.len()) {
                result.try_reserve(mapped.len_utf8())
                    .map_err(|_| Error::Execution(format!("{name} output allocation failed")))?;
            }
            result.push(mapped);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod fallible_unicode_case_reservation_tests {
    use super::*;
    #[test]
    fn uppercase_expansion_and_byte_budget_are_preserved() {
        let actual = convert_bounded("straße", 16, "UPPER",
            |text| text.to_ascii_uppercase(), |ch| ch.to_uppercase()).unwrap();
        assert_eq!(actual, "STRASSE");
        assert!(convert_bounded("ΐ", 3, "UPPER",
            |text| text.to_ascii_uppercase(), |ch| ch.to_uppercase()).is_err());
    }
}
