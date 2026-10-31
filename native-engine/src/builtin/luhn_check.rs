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
    let mut double = false;
    for digit in text.bytes().rev() {
        if !digit.is_ascii_digit() { return Ok(Value::Boolean(false)); }
        let value = usize::from(digit - b'0');
        checksum += if double { DOUBLED[value] } else { value as u32 };
        double = !double;
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
mod luhn_lookup_reference_tests {
    use super::*;

    #[test]
    fn agrees_with_arithmetic_reference_for_many_lengths() {
        for length in 1..=512_usize {
            let input = (0..length).map(|index|
                char::from(b'0' + ((index * 7 + length) % 10) as u8)
            ).collect::<String>();
            let sum: u32 = input.bytes().rev().enumerate().map(|(index, digit)| {
                let n = u32::from(digit - b'0');
                if index % 2 == 0 { n } else if n < 5 { n * 2 } else { n * 2 - 9 }
            }).sum();
            assert_eq!(evaluate(&[Value::Text(input)]).unwrap(),
                Value::Boolean(sum % 10 == 0));
        }
        assert_eq!(evaluate(&[Value::Text("123é".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
