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
    let bytes = text.as_bytes();
    if !bytes.iter().any(|byte| *byte == b'%' || *byte == b'+') {
        return Ok(text.to_owned());
    }
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1).copied().and_then(hex);
            let low = bytes.get(index + 2).copied().and_then(hex);
            let (Some(high), Some(low)) = (high, low) else {
                return Err(Error::Execution("invalid URL percent escape".into()));
            };
            decoded.push(high << 4 | low);
            index += 3;
        } else {
            decoded.push(if bytes[index] == b'+' { b' ' } else { bytes[index] });
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| Error::Execution("URL_DECODE produced invalid UTF-8".into()))
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
    }
}
