use super::{Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.len() == 1 && types[0] == DataType::Text { Ok(DataType::Boolean) }
    else { Err(Error::Type("LUHN_CHECK has incompatible SQL argument types".into())) }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null); };
    if text.is_empty() { return Ok(Value::Boolean(false)); }
    if text.len() > 16 * 1024 * 1024 {
        return Err(Error::Execution("LUHN_CHECK input exceeds native 16 MiB budget".into()));
    }
    const DOUBLED: [u32; 10] = [0, 2, 4, 6, 8, 1, 3, 5, 7, 9];
    let mut checksum = 0_u32;
    // Reverse chunks of two retain the fixed Luhn parity without an index test.
    let mut pairs = text.as_bytes().rchunks_exact(2);
    for pair in &mut pairs {
        let (left, right) = (pair[0], pair[1]);
        if !left.is_ascii_digit() || !right.is_ascii_digit() {
            return Ok(Value::Boolean(false));
        }
        checksum += DOUBLED[usize::from(left - b'0')] + u32::from(right - b'0');
    }
    for &digit in pairs.remainder() {
        if !digit.is_ascii_digit() { return Ok(Value::Boolean(false)); }
        checksum += u32::from(digit - b'0');
    }
    Ok(Value::Boolean(checksum % 10 == 0))
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
mod luhn_single_modulus_tests {
    use super::*;
    #[test]
    fn long_vectors_and_invalid_characters_preserve_checksum() {
        assert_eq!(evaluate(&[Value::Text("0".repeat(500_000))]).unwrap(),
            Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("79927398713".into())]).unwrap(),
            Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Text("79927398714".into())]).unwrap(),
            Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Text("7992a398713".into())]).unwrap(),
            Value::Boolean(false));
    }
}

#[cfg(test)]
mod paired_digit_checksum_tests {
    use super::*;

    #[test]
    fn matches_independent_reference_for_odd_even_and_long_inputs() {
        for len in 1..=128 {
            let digits = (0..len)
                .map(|index| char::from(b'0' + ((index * 7 + 3) % 10) as u8))
                .collect::<String>();
            let expected = digits.bytes().rev().enumerate().map(|(index, digit)| {
                let mut n = u32::from(digit - b'0');
                if index % 2 == 1 { n *= 2; if n > 9 { n -= 9; } }
                n
            }).sum::<u32>() % 10 == 0;
            assert_eq!(evaluate(&[Value::Text(digits)]).unwrap(),
                Value::Boolean(expected));
        }
        assert_eq!(evaluate(&[Value::Text("0".repeat(300_001))]).unwrap(),
            Value::Boolean(true));
        for invalid in ["1é", "1🙂2", " 123", "123-4", "a0"] {
            assert_eq!(evaluate(&[Value::Text(invalid.into())]).unwrap(),
                Value::Boolean(false));
        }
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
