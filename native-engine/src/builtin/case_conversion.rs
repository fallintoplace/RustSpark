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
    let mut result = String::with_capacity(text.len());
    for character in text.chars() {
        for mapped in unicode(character) {
            if mapped.len_utf8() > limit.saturating_sub(result.len()) {
                return Err(Error::Execution(format!("{name} output exceeds byte limit")));
            }
            result.push(mapped);
        }
    }
    Ok(result)
}
