use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    if text.len() > MAX_OUTPUT_BYTES {
        return Err(Error::Execution("URL_ENCODE input exceeds native 16 MiB budget".into()));
    }
    // Safe ASCII bytes pass through unchanged, including empty input.
    if text.bytes().all(|byte| matches!(byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*')) {
        return Ok(Value::Text(text.to_owned()));
    }
    let mut encoded = String::with_capacity(text.len());
    for &byte in text.as_bytes() {
        let escaped = !matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' | b' ');
        if encoded.len() > MAX_OUTPUT_BYTES - if escaped { 3 } else { 1 } {
            return Err(Error::Execution("URL_ENCODE output exceeds native 16 MiB budget".into()));
        }
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' =>
                encoded.push(char::from(byte)),
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
                encoded.push(char::from(DIGITS[usize::from(byte & 15)]));
            }
        }
    }
    Ok(Value::Text(encoded))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "url_encode", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encodes_utf8_bytes_and_form_spaces() {
        assert_eq!(evaluate(&[Value::Text("a b/é".into())]).unwrap(),
            Value::Text("a+b%2F%C3%A9".into()));
        assert_eq!(evaluate(&[Value::Text("A*-._09".into())]).unwrap(), Value::Text("A*-._09".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Text("🙂".repeat(1_400_000))]).is_err());
        assert!(evaluate(&[Value::Text("a".repeat(MAX_OUTPUT_BYTES + 1))]).is_err());
    }
}

#[cfg(test)]
mod url_encode_ascii_pass_through_tests {
    use super::*;
    #[test]
    fn passes_safe_ascii_and_escapes_reserved_and_unicode_bytes() {
        let safe = "aZ_-.09*".repeat(1024);
        assert_eq!(evaluate(&[Value::Text(safe.clone())]).unwrap(), Value::Text(safe));
        assert_eq!(evaluate(&[Value::Text("a~b".into())]).unwrap(),
            Value::Text("a%7Eb".into()));
        assert_eq!(evaluate(&[Value::Text("é+".into())]).unwrap(),
            Value::Text("%C3%A9%2B".into()));
    }
}
