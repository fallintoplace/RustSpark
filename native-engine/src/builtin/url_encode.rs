use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn is_url_safe(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*')
}

fn reserve_encoded(output: &mut Vec<u8>, additional: usize) -> Result<()> {
    output.len().checked_add(additional)
        .filter(|&len| len <= MAX_OUTPUT_BYTES)
        .ok_or_else(|| Error::Execution("URL_ENCODE output exceeds native 16 MiB budget".into()))?;
    output.try_reserve(additional)
        .map_err(|_| Error::Execution("URL_ENCODE output allocation failed".into()))
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    if text.len() > MAX_OUTPUT_BYTES {
        return Err(Error::Execution("URL_ENCODE input exceeds native 16 MiB budget".into()));
    }
    if text.bytes().all(is_url_safe) {
        let mut output = String::new();
        output.try_reserve_exact(text.len())
            .map_err(|_| Error::Execution("URL_ENCODE output allocation failed".into()))?;
        output.push_str(text);
        return Ok(Value::Text(output));
    }
    // Copy contiguous safe ASCII runs, encode exceptions, and check size
    // before appending. Avoid the two pre-scans and per-byte char pushes.
    let bytes = text.as_bytes();
    let mut output = Vec::new();
    let mut start = 0usize;
    for (index, &byte) in bytes.iter().enumerate() {
        if is_url_safe(byte) { continue; }
        let added = if byte == b' ' { 1 } else { 3 };
        let append = (index - start).checked_add(added)
            .ok_or_else(|| Error::Execution("URL_ENCODE output size overflow".into()))?;
        reserve_encoded(&mut output, append)?;
        output.extend_from_slice(&bytes[start..index]);
        if byte == b' ' {
            output.push(b'+');
        } else {
            output.extend_from_slice(&[
                b'%', DIGITS[usize::from(byte >> 4)],
                DIGITS[usize::from(byte & 15)],
            ]);
        }
        start = index + 1;
    }
    let remaining = bytes.len() - start;
    reserve_encoded(&mut output, remaining)?;
    output.extend_from_slice(&bytes[start..]);
    Ok(Value::Text(String::from_utf8(output)
        .expect("URL_ENCODE produces ASCII-only bytes")))
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

#[cfg(test)]
mod single_pass_url_encoding_tests {
    use super::*;

    #[test]
    fn encodes_long_safe_runs_and_multibyte_utf8_bytes() {
        let long = "a".repeat(250_000);
        let input = format!("{long}/é 🙂?{long}");
        let expected = format!("{long}%2F%C3%A9+%F0%9F%99%82%3F{long}");
        assert_eq!(evaluate(&[Value::Text(input)]).unwrap(), Value::Text(expected));
        assert_eq!(evaluate(&[Value::Text("+%&=".into())]).unwrap(),
            Value::Text("%2B%25%26%3D".into()));
    }

    #[test]
    fn checks_encoded_size_at_boundary_and_keeps_nulls() {
        let limit = MAX_OUTPUT_BYTES;
        assert_eq!(evaluate(&[Value::Text("a".repeat(limit))]).unwrap(),
            Value::Text("a".repeat(limit)));
        assert!(matches!(evaluate(&[Value::Text("/".repeat(limit / 3 + 1))]),
            Err(Error::Execution(_))));
        assert_eq!(evaluate(&[Value::Text(String::new())]).unwrap(),
            Value::Text(String::new()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod url_encode_safe_ascii_bypass_tests {
    use super::*;
    #[test]
    fn preserves_safe_bytes_without_changing_escape_behavior() {
        for input in ["", "ABCxyz0123456789", "-._*", "safe-Key_01.txt"] {
            assert_eq!(evaluate(&[Value::Text(input.into())]).unwrap(),
                Value::Text(input.into()));
        }
        assert_eq!(evaluate(&[Value::Text("A B".into())]).unwrap(),
            Value::Text("A+B".into()));
        assert_eq!(evaluate(&[Value::Text("é*".into())]).unwrap(),
            Value::Text("%C3%A9*".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        let safe = "a".repeat(100_000);
        assert_eq!(evaluate(&[Value::Text(safe.clone())]).unwrap(), Value::Text(safe));
    }
}

#[cfg(test)]
mod fallible_url_encoding_regressions {
    use super::*;

    #[test]
    fn grows_for_mixed_ascii_runs_unicode_and_escaped_bytes() {
        let run = "A".repeat(65_535);
        let input = format!("{run}/🙂 +{run}");
        let expected = format!("{run}%2F%F0%9F%99%82+%2B{run}");
        assert_eq!(evaluate(&[Value::Text(input)]).unwrap(), Value::Text(expected));
    }

    #[test]
    fn rejects_encoded_growth_beyond_limit_but_accepts_safe_boundary() {
        assert_eq!(evaluate(&[Value::Text("x".repeat(MAX_OUTPUT_BYTES))]).unwrap(),
            Value::Text("x".repeat(MAX_OUTPUT_BYTES)));
        assert!(evaluate(&[Value::Text("/".repeat(MAX_OUTPUT_BYTES / 3 + 1))]).is_err());
    }
}

#[cfg(test)]
mod encoded_growth_refactor_tests {
    use super::*;

    #[test]
    fn checks_growth_before_reserving_more_storage() {
        let mut output = vec![b'x'; 32];
        assert!(reserve_encoded(&mut output, MAX_OUTPUT_BYTES).is_err());
        reserve_encoded(&mut output, 4).unwrap();
        assert_eq!(output, vec![b'x'; 32]);
    }
}
