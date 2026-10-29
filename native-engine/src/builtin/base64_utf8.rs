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
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        output.push(DIGITS[(a >> 2) as usize] as char);
        output.push(DIGITS[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        output.push(if chunk.len() >= 2 { DIGITS[(((b & 15) << 2) | (c >> 6)) as usize] as char } else { '=' });
        output.push(if chunk.len() == 3 { DIGITS[(c & 63) as usize] as char } else { '=' });
    }
    Ok(Value::Text(output))
}
fn digit(c: u8) -> Result<u8> {
    match c {
        b'A'..=b'Z' => Ok(c - b'A'),
        b'a'..=b'z' => Ok(c - b'a' + 26),
        b'0'..=b'9' => Ok(c - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(Error::Parse("invalid BASE64 character".into())),
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
        assert!(unbase64_utf8(&[Value::Text("aGVsbG8==".into())]).is_err());
        assert!(unbase64_utf8(&[Value::Text("////".into())]).is_err());
        assert_eq!(base64(&[Value::Null]).unwrap(), Value::Null);
    }
}
