use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_BYTES: usize = 1_000_000;
const DIGITS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn base64(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let bytes = text.as_bytes();
    if bytes.len() > MAX_BYTES { return Err(Error::Execution("BASE64 input exceeds byte limit".into())); }
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut chunks = bytes.chunks_exact(3);
    for chunk in &mut chunks {
        let (a, b, c) = (chunk[0], chunk[1], chunk[2]);
        output.push(DIGITS[(a >> 2) as usize] as char);
        output.push(DIGITS[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        output.push(DIGITS[(((b & 15) << 2) | (c >> 6)) as usize] as char);
        output.push(DIGITS[(c & 63) as usize] as char);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        let a = tail[0];
        let b = if tail.len() == 2 { tail[1] } else { 0 };
        output.push(DIGITS[(a >> 2) as usize] as char);
        output.push(DIGITS[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        output.push(if tail.len() == 2 { DIGITS[((b & 15) << 2) as usize] as char } else { '=' });
        output.push('=');
    }
    Ok(Value::Text(output))
}
const REVERSE_DIGITS: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut index = 0;
    while index < DIGITS.len() {
        table[DIGITS[index] as usize] = index as u8;
        index += 1;
    }
    table
};

fn digit(c: u8) -> Result<u8> {
    let decoded = REVERSE_DIGITS[c as usize];
    if decoded == u8::MAX {
        Err(Error::Parse("invalid BASE64 character".into()))
    } else {
        Ok(decoded)
    }
}
fn unbase64_utf8(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let input = text.as_bytes();
    if input.len() % 4 != 0 || input.len() > MAX_BYTES.div_ceil(3) * 4 {
        return Err(Error::Parse("invalid or oversized BASE64 length".into()));
    }
    let mut bytes = Vec::with_capacity(input.len() / 4 * 3);
    let chunks = input.len() / 4;
    for (index, group) in input.chunks_exact(4).enumerate() {
        let padding = if group[2] == b'=' { 2 } else if group[3] == b'=' { 1 } else { 0 };
        if (padding != 0 && index + 1 != chunks) || group[0] == b'=' || group[1] == b'=' ||
            (padding == 2 && group[3] != b'=') {
            return Err(Error::Parse("invalid BASE64 padding".into()));
        }
        let a = digit(group[0])?;
        let b = digit(group[1])?;
        let c = if padding >= 2 { 0 } else { digit(group[2])? };
        let d = if padding >= 1 { 0 } else { digit(group[3])? };
        if (padding == 2 && (b & 15) != 0) || (padding == 1 && (c & 3) != 0) {
            return Err(Error::Parse("noncanonical BASE64 padding bits".into()));
        }
        bytes.push((a << 2) | (b >> 4));
        if padding < 2 { bytes.push((b << 4) | (c >> 2)); }
        if padding == 0 { bytes.push((c << 6) | d); }
    }
    let decoded = String::from_utf8(bytes)
        .map_err(|_| Error::Type("UNBASE64_UTF8 decoded bytes are not UTF-8".into()))?;
    Ok(Value::Text(decoded))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "base64", min_args: 1, max_args: 1, result_type: output, call: base64 },
    Function { name: "unbase64_utf8", min_args: 1, max_args: 1, result_type: output, call: unbase64_utf8 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn base64_round_trip_and_rejects_malformed_padding() {
        let encoded = base64(&[Value::Text("hello".into())]).unwrap();
        assert_eq!(encoded, Value::Text("aGVsbG8=".into()));
        assert_eq!(unbase64_utf8(&[encoded]).unwrap(), Value::Text("hello".into()));
        for text in ["", "a", "ab", "abc", "abcd", "abcde", "é", "🙂"] {
            let encoded = base64(&[Value::Text(text.into())]).unwrap();
            assert_eq!(unbase64_utf8(&[encoded]).unwrap(), Value::Text(text.into()));
        }
        assert!(unbase64_utf8(&[Value::Text("aGVsbG8==".into())]).is_err());
        assert!(unbase64_utf8(&[Value::Text("////".into())]).is_err());
        assert_eq!(base64(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod base64_reverse_table_perf_tests {
    use super::*;

    #[test]
    fn alphabet_table_and_padding_round_trip_utf8() {
        assert_eq!(unbase64_utf8(&[Value::Text("8J+Zgg==".into())]).unwrap(),
            Value::Text("🙂".into()));
        assert_eq!(unbase64_utf8(&[base64(&[Value::Text("abcXYZ+/".into())]).unwrap()]).unwrap(),
            Value::Text("abcXYZ+/".into()));
        assert!(unbase64_utf8(&[Value::Text("!!!!".into())]).is_err());
        assert!(unbase64_utf8(&[Value::Text("____".into())]).is_err());
    }
}
