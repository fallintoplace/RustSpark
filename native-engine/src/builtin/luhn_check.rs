use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Boolean) }
    else { Err(Error::Type("LUHN_CHECK has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.is_empty() { return Ok(Value::Boolean(false)); }
    let mut checksum = 0_u32;
    for (index, digit) in text.bytes().rev().enumerate() {
        if !digit.is_ascii_digit() { return Ok(Value::Boolean(false)); }
        let mut n = u32::from(digit - b'0');
        if index % 2 == 1 { n *= 2; if n > 9 { n -= 9; } }
        // Both terms are below ten, so at most one subtraction is needed.
        checksum += n;
        if checksum >= 10 { checksum -= 10; }
    }
    Ok(Value::Boolean(checksum == 0))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "luhn_check", min_args: 1, max_args: 1, result_type: output, call: evaluate },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spark_sql_semantics() {
        assert_eq!(evaluate(&[Value::Text("79927398713".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("79927398714".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("7x".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("1234x678".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("0".repeat(100_000))]).unwrap(),
            Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod luhn_bounded_checksum_tests {
    use super::*;
    #[test]
    fn matches_reference_modulo_on_many_digit_sequences() {
        fn reference(text: &str) -> bool {
            let mut checksum = 0_u32;
            for (index, byte) in text.bytes().rev().enumerate() {
                let mut n = u32::from(byte - b'0');
                if index % 2 == 1 { n *= 2; if n > 9 { n -= 9; } }
                checksum = (checksum + n) % 10;
            }
            checksum == 0
        }
        for length in 1..=150 {
            let text: String = (0..length).map(|index| char::from(b'0' + ((index * 7 + length) % 10) as u8)).collect();
            assert_eq!(evaluate(&[Value::Text(text.clone())]).unwrap(), Value::Boolean(reference(&text)));
        }
    }
}
