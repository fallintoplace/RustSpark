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
    let mut output = String::with_capacity(input.len() * 2);
    for &byte in input {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    Ok(Value::Text(output))
}
fn unhex_utf8(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    if text.len() % 2 != 0 || text.len() > MAX_INPUT_BYTES * 2 {
        return Err(Error::Parse("UNHEX_UTF8 requires bounded even-length hexadecimal text".into()));
    }
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let digits = std::str::from_utf8(pair)
            .map_err(|_| Error::Parse("UNHEX_UTF8 requires ASCII hex".into()))?;
        bytes.push(u8::from_str_radix(digits, 16)
            .map_err(|_| Error::Parse("invalid hexadecimal digit".into()))?);
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
