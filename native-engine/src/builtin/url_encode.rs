use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> { super::text_type(types) }
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(text.len());
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
    }
}
