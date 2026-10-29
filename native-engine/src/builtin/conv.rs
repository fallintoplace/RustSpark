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
    // 64 binary digits plus an optional sign fit in the stack buffer.
    let mut buffer = [0_u8; 65];
    let mut index = buffer.len();
    loop {
        index -= 1;
        buffer[index] = DIGITS[(value % base) as usize];
        value /= base;
        if value == 0 { break; }
    }
    if negative {
        index -= 1;
        buffer[index] = b'-';
    }
    String::from_utf8(buffer[index..].to_vec()).expect("CONV produces ASCII digits")
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let input = match &args[0] {
        Value::Text(text) => text.as_str(),
        Value::Integer(_) => "",
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("CONV requires text or integer input".into())),
    };
    let source = match &args[0] {
        Value::Integer(n) => n.to_string(),
        _ => input.to_string(),
    };
    let Some(from_base) = super::integer(&args[1])? else { return Ok(Value::Null) };
    let Some(to_base) = super::integer(&args[2])? else { return Ok(Value::Null) };
    let unsigned_base = to_base.unsigned_abs();
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&unsigned_base) {
        return Ok(Value::Null);
    }
    let raw = source.trim();
    if raw.len() > 128 { return Err(Error::Execution("CONV input exceeds 128-byte budget".into())); }
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
        assert!(evaluate(&[Value::Text("9".into()), Value::Integer(2), Value::Integer(10)]).is_err());
        assert_eq!(evaluate(&[Value::Null, Value::Integer(10), Value::Integer(2)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod conv_stack_format_tests {
    use super::*;
    #[test]
    fn formats_boundaries_in_binary_and_base_36() {
        assert_eq!(encode(u64::MAX, 2, false), "1".repeat(64));
        assert_eq!(encode(35, 36, false), "Z");
        assert_eq!(encode(0, 16, true), "-0");
    }
}
