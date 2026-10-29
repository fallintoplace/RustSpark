use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn ascii_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Text { Ok(DataType::Integer) }
    else { Err(Error::Type("ASCII requires text".into())) }
}
fn chr_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("CHR requires integer code point".into())) }
}
fn ascii(args: &[Value]) -> Result<Value> {
    match super::string(&args[0])? {
        None => Ok(Value::Null),
        Some(s) => Ok(Value::Integer(s.as_bytes().first().copied().unwrap_or(0) as i64)),
    }
}
fn chr(args: &[Value]) -> Result<Value> {
    let Some(code) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let ch = u32::try_from(code).ok().and_then(char::from_u32)
        .ok_or_else(|| Error::Execution("CHR code point is not a valid Unicode scalar".into()))?;
    Ok(Value::Text(ch.to_string()))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "ascii", min_args: 1, max_args: 1, result_type: ascii_type, call: ascii },
    Function { name: "chr", min_args: 1, max_args: 1, result_type: chr_type, call: chr },
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
