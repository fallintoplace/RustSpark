use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn ascii_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Text | DataType::Integer | DataType::Float) {
        Ok(DataType::Integer)
    } else { Err(Error::Type("ASCII requires text or numeric input".into())) }
}
fn chr_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Text) }
    else { Err(Error::Type("CHR requires integer code point".into())) }
}
fn ascii(args: &[Value]) -> Result<Value> {
    let byte = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Text(text) => text.as_bytes().first().copied().unwrap_or(0),
        Value::Integer(number) => {
            if *number < 0 { b'-' } else {
                let mut positive = *number as u64;
                while positive >= 10 { positive /= 10; }
                b'0' + positive as u8
            }
        },
        Value::Float(number) => number.to_string().as_bytes()[0],
        _ => return Err(Error::Type("ASCII requires text or numeric input".into())),
    };
    Ok(Value::Integer(i64::from(byte)))
}
fn chr(args: &[Value]) -> Result<Value> {
    let Some(code) = super::integer(&args[0])? else { return Ok(Value::Null) };
    // Spark CHR uses the low eight bits, not a Unicode scalar.
    let byte = code as u8;
    Ok(Value::Text(char::from(byte).to_string()))
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
        assert_eq!(chr(&[Value::Integer(0x1f642)]).unwrap(), Value::Text("B".into()));
        assert_eq!(chr(&[Value::Integer(321)]).unwrap(), Value::Text("A".into()));
        assert_eq!(chr(&[Value::Integer(-191)]).unwrap(), Value::Text("A".into()));
        assert_eq!(chr(&[Value::Integer(0xd800)]).unwrap(), Value::Text("\\0".into()));
        assert_eq!(chr(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(ascii_type(&[DataType::Integer]).unwrap(), DataType::Integer);
        assert_eq!(ascii(&[Value::Integer(42)]).unwrap(), Value::Integer(52));
        assert_eq!(ascii(&[Value::Integer(-42)]).unwrap(), Value::Integer(45));
        assert_eq!(ascii(&[Value::Float(3.14)]).unwrap(), Value::Integer(51));
    }
}

#[cfg(test)]
mod integer_ascii_first_byte_tests {
    use super::*;
    #[test]
    fn numeric_bounds_and_zero_keep_original_decimal_prefix() {
        assert_eq!(ascii(&[Value::Integer(0)]).unwrap(), Value::Integer(48));
        assert_eq!(ascii(&[Value::Integer(i64::MAX)]).unwrap(), Value::Integer(57));
        assert_eq!(ascii(&[Value::Integer(i64::MIN)]).unwrap(), Value::Integer(45));
        assert_eq!(ascii(&[Value::Integer(10_000)]).unwrap(), Value::Integer(49));
        assert_eq!(ascii(&[Value::Null]).unwrap(), Value::Null);
    }
}
