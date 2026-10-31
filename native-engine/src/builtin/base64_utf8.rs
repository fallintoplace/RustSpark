use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};
const MAX_BYTES: usize = 1_000_000;
const DIGITS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn base64(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let bytes = text.as_bytes();
    if bytes.len() > MAX_BYTES {
        return Err(Error::Execution("BASE64 input exceeds byte limit".into()));
    }
    // Emit each quartet as four ASCII bytes in one bulk append, then
    // validate UTF-8 once. Avoid four UTF-8 character pushes per 3-byte block.
    let mut output = Vec::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut triples = bytes.chunks_exact(3);
    for chunk in &mut triples {
        let (a, b, c) = (chunk[0], chunk[1], chunk[2]);
        output.extend_from_slice(&[
            DIGITS[(a >> 2) as usize],
            DIGITS[(((a & 3) << 4) | (b >> 4)) as usize],
            DIGITS[(((b & 15) << 2) | (c >> 6)) as usize],
            DIGITS[(c & 63) as usize],
        ]);
    }
    match triples.remainder() {
        [] => {}
        [a] => output.extend_from_slice(&[
            DIGITS[(a >> 2) as usize],
            DIGITS[((a & 3) << 4) as usize],
            b'=', b'=',
        ]),
        [a, b] => output.extend_from_slice(&[
            DIGITS[(a >> 2) as usize],
            DIGITS[(((a & 3) << 4) | (b >> 4)) as usize],
            DIGITS[((b & 15) << 2) as usize],
            b'=',
        ]),
        _ => unreachable!("chunks_exact leaves at most two bytes"),
    }
    Ok(Value::Text(String::from_utf8(output)
        .expect("BASE64 alphabet always contains valid ASCII")))
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
    if input.len() > MAX_BYTES.div_ceil(3) * 4 {
        return Err(Error::Execution("UNBASE64_UTF8 input exceeds byte budget".into()));
    }
    if input.len() % 4 != 0 {
        return Err(Error::Parse("invalid BASE64 length".into()));
    }
    let padding = usize::from(input.last() == Some(&b'='))
        + usize::from(input.get(input.len().saturating_sub(2)) == Some(&b'='));
    let decoded_len = input.len() / 4 * 3 - padding;
    if decoded_len > MAX_BYTES {
        return Err(Error::Execution("decoded BASE64 exceeds byte budget".into()));
    }
    let mut bytes = Vec::with_capacity(decoded_len);
    // Only the final quartet may be padded. Decode all preceding full
    // quartets with one path and no per-group last-element checks.
    let full_len = input.len().saturating_sub(4);
    let (full, tail) = input.split_at(full_len);
    for group in full.chunks_exact(4) {
        if group.contains(&b'=') {
            return Err(Error::Parse("invalid BASE64 padding".into()));
        }
        let a = digit(group[0])?;
        let b = digit(group[1])?;
        let c = digit(group[2])?;
        let d = digit(group[3])?;
        bytes.extend_from_slice(&[
            (a << 2) | (b >> 4),
            (b << 4) | (c >> 2),
            (c << 6) | d,
        ]);
    }
    if !tail.is_empty() {
        let padding = if tail[2] == b'=' { 2 } else if tail[3] == b'=' { 1 } else { 0 };
        if tail[0] == b'=' || tail[1] == b'=' ||
            (padding == 2 && tail[3] != b'=') {
            return Err(Error::Parse("invalid BASE64 padding".into()));
        }
        let a = digit(tail[0])?;
        let b = digit(tail[1])?;
        let c = if padding >= 2 { 0 } else { digit(tail[2])? };
        let d = if padding >= 1 { 0 } else { digit(tail[3])? };
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

#[cfg(test)]
mod decoded_output_budget_tests {
    use super::*;
    #[test]
    fn checks_decoded_size_instead_of_only_encoded_size() {
        let too_large = "AAAA".repeat(MAX_BYTES.div_ceil(3));
        assert!(matches!(unbase64_utf8(&[Value::Text(too_large)]), Err(Error::Execution(_))));
        assert!(matches!(
            unbase64_utf8(&[Value::Text("AAAA".repeat(MAX_BYTES.div_ceil(3) + 1))]),
            Err(Error::Execution(_)),
        ));
        let largest = "a".repeat(MAX_BYTES);
        let encoded = base64(&[Value::Text(largest.clone())]).unwrap();
        assert_eq!(unbase64_utf8(&[encoded]).unwrap(), Value::Text(largest));
    }
}

#[cfg(test)]
mod bulk_quartet_base64_regressions {
    use super::*;

    #[test]
    fn binary_boundaries_and_long_utf8_round_trip() {
        for len in 0..=256 {
            let text = "é🙂a".repeat(len);
            let original = Value::Text(text.clone());
            let encoded = base64(&[original.clone()]).unwrap();
            assert_eq!(unbase64_utf8(&[encoded.clone()]).unwrap(), original);
            let Value::Text(encoded) = encoded else { unreachable!() };
            assert_eq!(encoded.len(), text.len().div_ceil(3) * 4);
        }
        let largest = Value::Text("z".repeat(MAX_BYTES));
        assert_eq!(unbase64_utf8(&[base64(&[largest.clone()]).unwrap()]).unwrap(), largest);
        assert!(base64(&[Value::Text("z".repeat(MAX_BYTES + 1))]).is_err());
    }

    #[test]
    fn produces_canonical_padding_for_every_tail_size() {
        for (plain, encoded) in [
            ("", ""), ("a", "YQ=="), ("ab", "YWI="), ("abc", "YWJj"),
            ("abcd", "YWJjZA=="), ("abcde", "YWJjZGU="),
            ("abcdef", "YWJjZGVm"),
        ] {
            assert_eq!(base64(&[Value::Text(plain.into())]).unwrap(),
                Value::Text(encoded.into()));
        }
        assert_eq!(base64(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod full_quartet_decode_regressions {
    use super::*;

    #[test]
    fn handles_long_complete_groups_and_padded_tails() {
        for length in 0..=512 {
            let source = Value::Text("éa".repeat(length));
            let encoded = base64(&[source.clone()]).unwrap();
            assert_eq!(unbase64_utf8(&[encoded]).unwrap(), source);
        }
        assert_eq!(unbase64_utf8(&[Value::Text("YWJjZGVmZw==".into())]).unwrap(),
            Value::Text("abcdefg".into()));
        assert!(unbase64_utf8(&[Value::Text("YWJjZGVmZ=== ".into())]).is_err());
        assert!(unbase64_utf8(&[Value::Text("YWI=".into())]).is_ok());
        assert_eq!(unbase64_utf8(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bulk_base64_decode_tests {
    use super::*;

    #[test]
    fn all_tail_lengths_and_long_unicode_round_trip() {
        for repeat in 0..128 {
            let original = Value::Text(format!("{}é🙂", "xyz".repeat(repeat)));
            let encoded = base64(&[original.clone()]).unwrap();
            assert_eq!(unbase64_utf8(&[encoded]).unwrap(), original);
        }
        for invalid in ["a===", "YWJj=AAA", "AA==AAAA", "Zm9=", "Zh=="] {
            assert!(unbase64_utf8(&[Value::Text(invalid.into())]).is_err(), "{invalid}");
        }
        assert_eq!(unbase64_utf8(&[Value::Text(String::new())]).unwrap(),
            Value::Text(String::new()));
    }
}
