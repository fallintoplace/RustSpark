use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_INPUT_BYTES: usize = 1_000_000;
const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
// Two output bytes per input byte, computed at compile time.
const HEX_PAIRS: [[u8; 2]; 256] = {
    let mut pairs = [[0; 2]; 256];
    let mut index = 0;
    while index < pairs.len() {
        pairs[index] = [DIGITS[index >> 4], DIGITS[index & 15]];
        index += 1;
    }
    pairs
};

fn hex_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Text | DataType::Integer) { Ok(DataType::Text) }
    else { Err(Error::Type("HEX requires text or integer".into())) }
}
fn unhex_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)
}
fn hex(args: &[Value]) -> Result<Value> {
    let input = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Integer(v) => return Ok(Value::Text(format!("{v:X}"))),
        Value::Text(v) => v.as_bytes(),
        _ => return Err(Error::Type("HEX requires text or integer".into())),
    };
    if input.len() > MAX_INPUT_BYTES {
        return Err(Error::Execution("HEX input exceeds byte limit".into()));
    }
    // Append both hexadecimal digits in one bulk copy, without converting
    // bytes to char or performing two UTF-8 pushes for each input byte.
    let mut output = Vec::with_capacity(input.len() * 2);
    for &byte in input {
        output.extend_from_slice(&HEX_PAIRS[usize::from(byte)]);
    }
    Ok(Value::Text(String::from_utf8(output)
        .expect("HEX pairs contain only ASCII bytes")))
}
const UNHEX_DIGITS: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut digit = 0_u8;
    while digit < 10 {
        table[(b'0' + digit) as usize] = digit;
        digit += 1;
    }
    let mut digit = 0_u8;
    while digit < 6 {
        table[(b'A' + digit) as usize] = digit + 10;
        table[(b'a' + digit) as usize] = digit + 10;
        digit += 1;
    }
    table
};

fn decode_hex_digit(byte: u8) -> Result<u8> {
    let digit = UNHEX_DIGITS[usize::from(byte)];
    if digit == u8::MAX {
        Err(Error::Parse("invalid hexadecimal digit".into()))
    } else {
        Ok(digit)
    }
}

fn unhex_utf8(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    if text.len() > MAX_INPUT_BYTES * 2 {
        return Err(Error::Execution("UNHEX_UTF8 input exceeds byte budget".into()));
    }
    if text.len() % 2 != 0 {
        return Err(Error::Parse("UNHEX_UTF8 requires even-length hexadecimal text".into()));
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let upper = decode_hex_digit(pair[0])?;
        let lower = decode_hex_digit(pair[1])?;
        bytes.push((upper << 4) | lower);
    }
    let decoded = String::from_utf8(bytes)
        .map_err(|_| Error::Type("UNHEX_UTF8 decoded bytes are not valid UTF-8".into()))?;
    Ok(Value::Text(decoded))
}
fn try_unhex_utf8(args: &[Value]) -> Result<Value> {
    match unhex_utf8(args) {
        Ok(value) => Ok(value),
        Err(Error::Parse(_) | Error::Type(_)) => Ok(Value::Null),
        Err(error) => Err(error),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "try_unhex_utf8", min_args: 1, max_args: 1,
        result_type: unhex_type, call: try_unhex_utf8 },
    Function { name: "hex", min_args: 1, max_args: 1, result_type: hex_type, call: hex },
    Function { name: "unhex_utf8", min_args: 1, max_args: 1, result_type: unhex_type, call: unhex_utf8 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hex_round_trips_unicode_with_utf8_only_decode() {
        let encoded = hex(&[Value::Text("café".into())]).unwrap();
        assert_eq!(encoded, Value::Text("636166C3A9".into()));
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), Value::Text("café".into()));
        assert_eq!(hex(&[Value::Integer(255)]).unwrap(), Value::Text("FF".into()));
        assert!(unhex_utf8(&[Value::Text("FF".into())]).is_err());
        assert!(unhex_utf8(&[Value::Text("A".into())]).is_err());
    }
}

#[cfg(test)]
mod unhex_byte_decode_perf_tests {
    use super::*;
    #[test]
    fn decodes_mixed_case_hex_and_rejects_invalid_bytes() {
        assert_eq!(unhex_utf8(&[Value::Text("48656c6c6F".into())]).unwrap(),
            Value::Text("Hello".into()));
        assert_eq!(unhex_utf8(&[Value::Text("".into())]).unwrap(),
            Value::Text("".into()));
        assert!(unhex_utf8(&[Value::Text("00GG".into())]).is_err());
        assert!(unhex_utf8(&[Value::Text("c3a9".into())]).is_ok());
    }
}

#[cfg(test)]
mod hex_byte_buffer_perf_tests {
    use super::*;
    #[test]
    fn encodes_empty_ascii_unicode_and_long_strings() {
        assert_eq!(hex(&[Value::Text("".into())]).unwrap(), Value::Text("".into()));
        assert_eq!(hex(&[Value::Text("A".into())]).unwrap(), Value::Text("41".into()));
        assert_eq!(hex(&[Value::Text("é🙂".into())]).unwrap(), Value::Text("C3A9F09F9982".into()));
        let input = Value::Text("x".repeat(1024));
        let encoded = hex(&[input.clone()]).unwrap();
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), input);
    }
}

#[cfg(test)]
mod try_unhex_utf8_extension_tests {
    use super::*;
    #[test]
    fn returns_null_for_invalid_encoding_and_preserves_unicode() {
        assert!(crate::builtin::supports("try_unhex_utf8"));
        assert_eq!(try_unhex_utf8(&[Value::Text("c3a9".into())]).unwrap(), Value::Text("é".into()));
        assert_eq!(try_unhex_utf8(&[Value::Text("FF".into())]).unwrap(), Value::Null);
        assert_eq!(try_unhex_utf8(&[Value::Text("GG".into())]).unwrap(), Value::Null);
        assert_eq!(try_unhex_utf8(&[Value::Null]).unwrap(), Value::Null);
        assert!(matches!(
            try_unhex_utf8(&[Value::Text("00".repeat(MAX_INPUT_BYTES + 1))]),
            Err(Error::Execution(_)),
        ));
    }
}

#[cfg(test)]
mod hex_direct_string_tests {
    use super::*;

    #[test]
    fn encodes_unicode_bytes_without_a_utf8_conversion() {
        assert_eq!(hex(&[Value::Text("é🙂".into())]).unwrap(),
            Value::Text("C3A9F09F9982".into()));
        assert_eq!(hex(&[Value::Text(String::new())]).unwrap(),
            Value::Text(String::new()));
    }
}

#[cfg(test)]
mod hex_pair_lookup_tests {
    use super::*;

    #[test]
    fn every_ascii_value_round_trips_and_uses_uppercase_pairs() {
        let input = (0u8..=127).map(char::from).collect::<String>();
        let expected = (0u8..=127)
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        let encoded = hex(&[Value::Text(input.clone())]).unwrap();
        assert_eq!(encoded, Value::Text(expected));
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), Value::Text(input));
    }

    #[test]
    fn large_unicode_payload_maintains_byte_count_and_decode_semantics() {
        let source = Value::Text("é🙂界a".repeat(10_000));
        let encoded = hex(&[source.clone()]).unwrap();
        let Value::Text(ref encoded_text) = encoded else { unreachable!() };
        assert_eq!(encoded_text.len(), 10_000 * "é🙂界a".len() * 2);
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), source);
        assert_eq!(hex(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bulk_hex_encoding_regressions {
    use super::*;

    #[test]
    fn every_byte_and_long_unicode_strings_round_trip() {
        let bytes = (0..=127u8).collect::<Vec<_>>();
        let input = String::from_utf8(bytes).unwrap();
        let encoded = hex(&[Value::Text(input.clone())]).unwrap();
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), Value::Text(input));
        let wide = Value::Text("🙂é東京".repeat(10_000));
        let encoded = hex(&[wide.clone()]).unwrap();
        assert_eq!(unhex_utf8(&[encoded]).unwrap(), wide);
        assert_eq!(hex(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod unhex_nibble_table_tests {
    use super::*;

    #[test]
    fn decodes_every_ascii_byte_and_mixed_case_hex() {
        for byte in 0_u8..=127 {
            let encoded = format!("{byte:02X}");
            assert_eq!(
                unhex_utf8(&[Value::Text(encoded)]).unwrap(),
                Value::Text(char::from(byte).to_string())
            );
        }
        assert_eq!(
            unhex_utf8(&[Value::Text("c3A9F09f9982".into())]).unwrap(),
            Value::Text("é🙂".into())
        );
        for invalid in ["0g", "G0", "_0", "é0", "0🙂"] {
            assert!(unhex_utf8(&[Value::Text(invalid.into())]).is_err());
        }
        assert_eq!(unhex_utf8(&[Value::Null]).unwrap(), Value::Null);
    }
}
