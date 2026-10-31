use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn decode(text: &str) -> Result<String> {
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("URL_DECODE input exceeds native 16 MiB budget".into()));
    }
    let bytes = text.as_bytes();
    // Check both escape forms in one pass. Most URL values are already
    // decoded, so avoid scanning them twice before returning the copy.
    let Some(first_escape) = bytes.iter()
        .position(|&byte| byte == b'%' || byte == b'+') else {
        return Ok(text.to_owned());
    };
    let mut decoded = Vec::with_capacity(bytes.len());
    decoded.extend_from_slice(&bytes[..first_escape]);
    let mut index = first_escape;
    let mut start = first_escape;
    while index < bytes.len() {
        if bytes[index] != b'%' && bytes[index] != b'+' {
            index += 1;
            continue;
        }
        // Retain unchanged UTF-8 byte spans without pushing one byte at a time.
        decoded.extend_from_slice(&bytes[start..index]);
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1).copied().and_then(hex);
            let low = bytes.get(index + 2).copied().and_then(hex);
            let (Some(high), Some(low)) = (high, low) else {
                return Err(Error::Parse("invalid URL percent escape".into()));
            };
            decoded.push(high << 4 | low);
            index += 3;
        } else {
            decoded.push(b' ');
            index += 1;
        }
        start = index;
    }
    decoded.extend_from_slice(&bytes[start..]);
    String::from_utf8(decoded)
        .map_err(|_| Error::Type("URL_DECODE produced invalid UTF-8".into()))
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Text(decode(text)?))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "url_decode", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_percent_escapes_plus_and_unicode() {
        assert_eq!(evaluate(&[Value::Text("a+b%2F%C3%A9".into())]).unwrap(), Value::Text("a b/é".into()));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert!(decode("%GG").is_err());
        assert!(decode("%FF").is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(decode(&"a".repeat(16 * 1024 * 1024 + 1)).is_err());
    }
}

#[cfg(test)]
mod url_plus_only_decode_tests {
    use super::*;
    #[test]
    fn preserves_utf8_and_literal_plus_encoding() {
        assert_eq!(decode("café+東京+ok").unwrap(), "café 東京 ok");
        assert_eq!(decode("café東京").unwrap(), "café東京");
        assert_eq!(decode("a%2Bb+c").unwrap(), "a+b c");
        assert!(decode("a%GG+b").is_err());
    }
}

#[cfg(test)]
mod url_decode_untouched_run_tests {
    use super::*;

    #[test]
    fn keeps_long_unicode_runs_while_decoding_sparse_escapes() {
        let run = "é🙂界".repeat(12_000);
        let encoded = format!("{run}%20{run}%2B{run}");
        let expected = format!("{run} {run}+{run}");
        assert_eq!(decode(&encoded).unwrap(), expected);
        assert_eq!(decode("a%252Bb").unwrap(), "a%2Bb");
    }

    #[test]
    fn preserves_form_spaces_and_rejects_invalid_utf8_and_escapes() {
        assert_eq!(decode("+a+b+").unwrap(), " a b ");
        assert_eq!(decode("a%2Bb+%C3%A9").unwrap(), "a+b é");
        for invalid in ["%", "%A", "%GG", "%FF", "%E2%28%A1"] {
            assert!(decode(invalid).is_err(), "{invalid}");
        }
        assert_eq!(decode("").unwrap(), "");
        assert_eq!(decode("safeASCII").unwrap(), "safeASCII");
    }
}

#[cfg(test)]
mod first_special_byte_decode_regressions {
    use super::*;

    #[test]
    fn preserves_utf8_prefixes_plus_signs_and_percent_escapes() {
        for (input, output) in [
            ("", ""), ("plain", "plain"), ("é🙂東京", "é🙂東京"),
            ("é🙂+東京", "é🙂 東京"), ("prefix%20suffix", "prefix suffix"),
            ("é🙂%2B%20東京", "é🙂+ 東京"), ("%25+%25", "% %"),
            ("a%2520b", "a%20b"), ("++", "  "),
        ] {
            assert_eq!(decode(input).unwrap(), output, "{input}");
        }
        let prefix = "東京🙂".repeat(20_000);
        assert_eq!(decode(&format!("{prefix}+%2B")).unwrap(),
            format!("{prefix} +"));
        for invalid in ["%", "%0", "%G1", "%FF", "a+%G1"] {
            assert!(decode(invalid).is_err(), "{invalid}");
        }
    }
}
