use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn ascii_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Text | DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("ASCII requires text".into())) }
}
fn chr_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("CHR requires integer code point".into())) }
}
fn ascii(args: &[Value]) -> Result<Value> {
    let text = match &args[0] {
        Value::Text(text) => text.clone(),
        Value::Integer(value) => value.to_string(),
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("ASCII requires text or integer".into())),
    };
    Ok(Value::Integer(i64::from(text.as_bytes().first().copied().unwrap_or(0))))
}
fn chr(args: &[Value]) -> Result<Value> {
    let Some(code) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let normalized = code.rem_euclid(256) as u8;
    if normalized >= 128 {
        return Err(Error::Unsupported("CHR non-ASCII bytes require binary string support".into()));
    }
    Ok(Value::Text(char::from(normalized).to_string()))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "ascii", min_args: 1, max_args: 1, result_type: ascii_type, call: ascii },
    Function { name: "chr", min_args: 1, max_args: 1, result_type: chr_type, call: chr },
    Function { name: "char", min_args: 1, max_args: 1, result_type: chr_type, call: chr },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ascii_first_byte_and_unicode_chr() {
        assert_eq!(ascii(&[Value::Text("Apple".into())]).unwrap(), Value::Integer(65));
        assert_eq!(ascii(&[Value::Text("".into())]).unwrap(), Value::Integer(0));
        assert_eq!(chr(&[Value::Integer(0x1f642)]).unwrap(), Value::Text("🙂".into()));
        assert!(chr(&[Value::Integer(0xd800)]).is_err());
        assert_eq!(chr(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod spark_ascii_chr_tests {
    use super::*;

    #[test]
    fn ascii_supports_numeric_inputs_and_utf8_first_bytes() {
        assert_eq!(ascii(&[Value::Integer(222)]).unwrap(), Value::Integer(50));
        assert_eq!(ascii(&[Value::Text("é".into())]).unwrap(), Value::Integer(195));
        assert_eq!(ascii(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn chr_wraps_ascii_values_modulo_256() {
        assert_eq!(chr(&[Value::Integer(65)]).unwrap(), Value::Text("A".into()));
        assert_eq!(chr(&[Value::Integer(321)]).unwrap(), Value::Text("A".into()));
        assert_eq!(chr(&[Value::Integer(-191)]).unwrap(), Value::Text("A".into()));
        assert_eq!(chr(&[Value::Null]).unwrap(), Value::Null);
        assert!(chr(&[Value::Integer(255)]).is_err());
     }

 }
