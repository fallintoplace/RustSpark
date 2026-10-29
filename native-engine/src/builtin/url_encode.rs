use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const HEX: &[u8; 16] = b"0123456789ABCDEF";
fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("URL_ENCODE requires a text argument".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.len().checked_mul(3).is_none_or(|n| n > 16 * 1024 * 1024) {
        return Err(Error::Execution("URL_ENCODE exceeds native 16 MiB budget".into()));
    }
    let mut result = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'*') {
            result.push(char::from(byte));
        } else if byte == b' ' {
            result.push('+');
        } else {
            result.push('%');
            result.push(char::from(HEX[usize::from(byte >> 4)]));
            result.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    Ok(Value::Text(result))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "url_encode", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("a b+c/é".into())]).unwrap(), Value::Text("a+b%2Bc%2F%C3%A9".into()));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
