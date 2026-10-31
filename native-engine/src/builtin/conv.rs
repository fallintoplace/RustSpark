use std::borrow::Cow;
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Text | DataType::Integer) &&
        types[1] == DataType::Integer && types[2] == DataType::Integer {
        Ok(DataType::Text)
    } else { Err(Error::Type("CONV requires text/integer and two integer bases".into())) }
}
fn encode(mut value: u64, base: u64, negative: bool) -> String {
    const DIGITS: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    // At most 64 binary digits plus one sign; no heap allocation until output.
    let mut digits = [0_u8; 65];
    let mut cursor = digits.len();
    loop {
        cursor -= 1;
        digits[cursor] = DIGITS[(value % base) as usize];
        value /= base;
        if value == 0 { break; }
    }
    if negative {
        cursor -= 1;
        digits[cursor] = b'-';
    }
    std::str::from_utf8(&digits[cursor..])
        .expect("CONV emits only ASCII digits").to_owned()
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let source: Cow<'_, str> = match &args[0] {
        Value::Text(text) => Cow::Borrowed(text),
        Value::Integer(n) => Cow::Owned(n.to_string()),
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("CONV requires text or integer input".into())),
    };
    let Some(from_base) = super::integer(&args[1])? else { return Ok(Value::Null) };
    let Some(to_base) = super::integer(&args[2])? else { return Ok(Value::Null) };
    let unsigned_base = to_base.unsigned_abs();
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&unsigned_base) {
        return Ok(Value::Null);
    }
    if source.len() > 128 { return Err(Error::Execution("CONV input exceeds 128-byte budget".into())); }
    let raw = source.trim();
    let (negative, raw) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    if raw.is_empty() { return Ok(Value::Null); }
    let magnitude = u64::from_str_radix(raw, from_base as u32)
        .map_err(|_| Error::Execution("CONV input is not a valid 64-bit numeral".into()))?;
    let bits = if negative { magnitude.wrapping_neg() } else { magnitude };
    let signed = to_base < 0 && (bits as i64) < 0;
    let value = if signed { (bits as i64).unsigned_abs() } else { bits };
    Ok(Value::Text(encode(value, unsigned_base, signed)))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "conv", min_args: 3, max_args: 3, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts_between_signed_and_unsigned_bases() {
        assert_eq!(evaluate(&[Value::Text("100".into()), Value::Integer(2), Value::Integer(10)]).unwrap(), Value::Text("4".into()));
        assert_eq!(evaluate(&[Value::Text("FF".into()), Value::Integer(16), Value::Integer(10)]).unwrap(), Value::Text("255".into()));
        assert_eq!(evaluate(&[Value::Integer(-10), Value::Integer(16), Value::Integer(-10)]).unwrap(), Value::Text("-16".into()));
        assert_eq!(evaluate(&[Value::Text("-1".into()), Value::Integer(10), Value::Integer(16)]).unwrap(), Value::Text("FFFFFFFFFFFFFFFF".into()));
        assert_eq!(evaluate(&[Value::Text("0".into()), Value::Integer(10), Value::Integer(2)]).unwrap(), Value::Text("0".into()));
        assert_eq!(evaluate(&[Value::Text("18446744073709551615".into()), Value::Integer(10), Value::Integer(2)]).unwrap(),
            Value::Text("1".repeat(64)));
        assert!(evaluate(&[Value::Text("9".into()), Value::Integer(2), Value::Integer(10)]).is_err());
        // Leading whitespace must not bypass the source byte budget.
        assert!(evaluate(&[Value::Text(format!("{}FF", " ".repeat(129))),
            Value::Integer(16), Value::Integer(10)]).is_err());
        assert_eq!(evaluate(&[Value::Text(" FF ".into()), Value::Integer(16),
            Value::Integer(10)]).unwrap(), Value::Text("255".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(10), Value::Integer(2)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod borrowed_conv_text_tests {
    use super::*;
    #[test]
    fn converts_borrowed_text_without_changing_signed_semantics() {
        assert_eq!(evaluate(&[Value::Text("  FF  ".into()), Value::Integer(16), Value::Integer(10)]).unwrap(),
            Value::Text("255".into()));
        assert_eq!(evaluate(&[Value::Integer(-10), Value::Integer(16), Value::Integer(-10)]).unwrap(),
            Value::Text("-16".into()));
        assert_eq!(evaluate(&[Value::Text("not-a-number".into()), Value::Integer(10), Value::Integer(16)]).is_err(), true);
    }
}
