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
    // No escaping or form-space substitution is needed for safe ASCII.
    if text.bytes().all(|byte| matches!(byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*')) {
        return Ok(Value::Text(text.to_owned()));
    }
    let mut output_bytes = 0usize;
    for &byte in text.as_bytes() {
        let emitted = if matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' | b' ') { 1 } else { 3 };
        output_bytes = output_bytes.checked_add(emitted)
            .filter(|&bytes| bytes <= MAX_OUTPUT_BYTES)
            .ok_or_else(|| Error::Execution("URL_ENCODE output exceeds native 16 MiB budget".into()))?;
    }
    let mut encoded = String::with_capacity(output_bytes);
    for &byte in text.as_bytes() {
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
mod url_encode_exact_capacity_tests {
    use super::*;
    #[test]
    fn handles_large_escaped_text_and_output_overflow() {
        let input = "/".repeat(8192);
        assert_eq!(evaluate(&[Value::Text(input)]).unwrap(), Value::Text("%2F".repeat(8192)));
        assert_eq!(evaluate(&[Value::Text("é 🙂".into())]).unwrap(), Value::Text("%C3%A9+%F0%9F%99%82".into()));
        assert!(evaluate(&[Value::Text("/".repeat(MAX_OUTPUT_BYTES / 3 + 1))]).is_err());
    }
}

#[cfg(test)]
mod unescaped_ascii_url_fast_path_tests {
    use super::*;
    #[test]
    fn preserves_url_safe_bytes_and_encodes_special_bytes() {
        assert_eq!(evaluate(&[Value::Text("Aaz09-._*".into())]).unwrap(),
            Value::Text("Aaz09-._*".into()));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(evaluate(&[Value::Text("a+b c".into())]).unwrap(),
            Value::Text("a%2Bb+c".into()));
        assert_eq!(evaluate(&[Value::Text("é".into())]).unwrap(),
            Value::Text("%C3%A9".into()));
    }
}
