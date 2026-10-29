use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_INPUT_BYTES: usize = 1_000_000;
const DIGITS: &[u8; 16] = b"0123456789ABCDEF";

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
    let mut output = Vec::with_capacity(input.len() * 2);
    for &byte in input {
        output.push(DIGITS[(byte >> 4) as usize]);
        output.push(DIGITS[(byte & 15) as usize]);
    }
    Ok(Value::Text(String::from_utf8(output).expect("HEX produces ASCII digits")))
}
const REVERSE_DIGITS: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut index = 0;
    while index < 16 {
        let upper = DIGITS[index] as usize;
        table[upper] = index as u8;
        if index >= 10 {
            table[upper + (b'a' - b'A') as usize] = index as u8;
        }
        index += 1;
    }
    table
};
fn decode_hex_digit(byte: u8) -> Result<u8> {
    let value = REVERSE_DIGITS[usize::from(byte)];
    if value == u8::MAX {
        Err(Error::Parse("invalid hexadecimal digit".into()))
    } else {
        Ok(value)
    }
}

fn unhex_utf8(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    if text.len() % 2 != 0 || text.len() > MAX_INPUT_BYTES * 2 {
        return Err(Error::Parse("UNHEX_UTF8 requires bounded even-length hexadecimal text".into()));
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
pub(super) const FUNCTIONS: &[Function] = &[
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
mod reverse_hex_digit_table_tests {
    use super::*;
    #[test]
    fn matches_all_ascii_hex_digits_and_rejects_invalid_bytes() {
        for digit in b'0'..=b'9' { assert_eq!(decode_hex_digit(digit).unwrap(), digit - b'0'); }
        for offset in 0..6u8 {
            assert_eq!(decode_hex_digit(b'a' + offset).unwrap(), offset + 10);
            assert_eq!(decode_hex_digit(b'A' + offset).unwrap(), offset + 10);
        }
        for digit in [b'/', b':', b'G', b'g', b' ', 0xff] { assert!(decode_hex_digit(digit).is_err()); }
        assert_eq!(unhex_utf8(&[Value::Text("C3a9".into())]).unwrap(), Value::Text("é".into()));
    }
}
