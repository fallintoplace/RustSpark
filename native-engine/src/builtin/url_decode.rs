use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn hex_digit(byte: u8) -> Option<u8> {
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
    let input = text.as_bytes();
    let mut bytes = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%' {
            if index + 2 >= input.len() {
                return Err(Error::Execution("URL_DECODE has incomplete percent escape".into()));
            }
            let hi = hex_digit(input[index + 1])
                .ok_or_else(|| Error::Execution("URL_DECODE has invalid percent escape".into()))?;
            let lo = hex_digit(input[index + 2])
                .ok_or_else(|| Error::Execution("URL_DECODE has invalid percent escape".into()))?;
            bytes.push((hi << 4) | lo);
            index += 3;
        } else {
            bytes.push(if input[index] == b'+' { b' ' } else { input[index] });
            index += 1;
        }
    }
    String::from_utf8(bytes).map_err(|_| Error::Execution("URL_DECODE produced invalid UTF-8".into()))
}
fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Text) }
    else { Err(Error::Type("URL_DECODE requires a text argument".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Text(decode(text)?))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "url_decode", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("a+b%2Bc%2F%C3%A9".into())]).unwrap(), Value::Text("a b+c/é".into()));
        assert!(evaluate(&[Value::Text("%GG".into())]).is_err());
        assert!(evaluate(&[Value::Text("%FF".into())]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
