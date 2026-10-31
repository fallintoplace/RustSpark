use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("FORMAT_NUMBER requires number and integer precision".into()))
    }
}
fn grouped_integer(value: i64, precision: usize) -> String {
    let mut magnitude = value.unsigned_abs();
    let mut reversed = [0u8; 20];
    let mut len = 0usize;
    loop {
        reversed[len] = b'0' + (magnitude % 10) as u8;
        len += 1;
        magnitude /= 10;
        if magnitude == 0 { break; }
    }
    let fraction = if precision == 0 { 0 } else { precision + 1 };
    let mut result = String::with_capacity(
        len + (len - 1) / 3 + usize::from(value < 0) + fraction,
    );
    if value < 0 { result.push('-'); }
    for digit in (0..len).rev() {
        result.push(char::from(reversed[digit]));
        if digit > 0 && digit % 3 == 0 { result.push(','); }
    }
    if precision > 0 {
        const ZEROS: &str = "000000000000000";
        result.push('.');
        result.push_str(&ZEROS[..precision]);
    }
    result
}

fn evaluate(args: &[Value]) -> Result<Value> {
    if args[0].is_null() { return Ok(Value::Null); }
    if !matches!(args[0], Value::Integer(_) | Value::Float(_)) {
        return Err(Error::Type("FORMAT_NUMBER requires a number".into()));
    }
    let Some(digits) = super::integer(&args[1])? else { return Ok(Value::Null) };
    if digits < 0 { return Ok(Value::Null) }
    let digits = usize::try_from(digits)
        .map_err(|_| Error::Execution("FORMAT_NUMBER precision overflow".into()))?;
    if digits > 15 { return Err(Error::Execution("FORMAT_NUMBER supports at most 15 decimal places".into())); }
    if let Value::Integer(value) = args[0] {
        // A 64-bit integer has at most twenty decimal digits. Format directly
        // into the final output without allocating an intermediate string.
        return Ok(Value::Text(grouped_integer(value, digits)));
    }
    let formatted = match args[0] {
        Value::Float(value) if value.is_finite() => format!("{value:.digits$}"),
        Value::Float(_) => return Err(Error::Execution("FORMAT_NUMBER requires finite number".into())),
        _ => unreachable!("numeric input already validated"),
    };
    let (negative, unsigned) = formatted.strip_prefix('-')
        .map_or(("", formatted.as_str()), |rest| ("-", rest));
    let (whole, fractional) = unsigned.split_once('.')
        .map_or((unsigned, None), |(whole, fractional)| (whole, Some(fractional)));
    let mut result = String::with_capacity(formatted.len().saturating_add(whole.len() / 3)
        .saturating_add(digits + 1));
    result.push_str(negative);
    for (index, byte) in whole.bytes().enumerate() {
        if index > 0 && (whole.len() - index) % 3 == 0 {
            result.push(',');
        }
        result.push(char::from(byte));
    }
    if let Some(fractional) = fractional {
        result.push('.');
        result.push_str(fractional);
    }
    Ok(Value::Text(result))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "format_number", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn groups_thousands_and_rounds_decimal_places() {
        assert_eq!(evaluate(&[Value::Float(12332.123456), Value::Integer(4)]).unwrap(), Value::Text("12,332.1235".into()));
        assert_eq!(evaluate(&[Value::Integer(-12345), Value::Integer(0)]).unwrap(), Value::Text("-12,345".into()));
        assert_eq!(evaluate(&[Value::Integer(15), Value::Integer(2)]).unwrap(), Value::Text("15.00".into()));
        assert_eq!(evaluate(&[Value::Integer(1), Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod grouped_number_single_output_perf_tests {
    use super::*;

    #[test]
    fn retains_negative_signs_fractional_zeros_and_large_groups() {
        assert_eq!(evaluate(&[Value::Integer(1_234_567), Value::Integer(0)]).unwrap(),
            Value::Text("1,234,567".into()));
        assert_eq!(evaluate(&[Value::Integer(-1_234_567), Value::Integer(2)]).unwrap(),
            Value::Text("-1,234,567.00".into()));
        assert_eq!(evaluate(&[Value::Integer(0), Value::Integer(3)]).unwrap(),
            Value::Text("0.000".into()));
    }
    #[test]
    fn preserves_64_bit_integer_precision_without_float_roundtrip() {
        assert_eq!(evaluate(&[Value::Integer(9_007_199_254_740_993), Value::Integer(0)]).unwrap(),
            Value::Text("9,007,199,254,740,993".into()));
        assert_eq!(evaluate(&[Value::Integer(i64::MAX), Value::Integer(2)]).unwrap(),
            Value::Text("9,223,372,036,854,775,807.00".into()));
        assert_eq!(evaluate(&[Value::Integer(i64::MIN), Value::Integer(0)]).unwrap(),
            Value::Text("-9,223,372,036,854,775,808".into()));
    }

}

#[cfg(test)]
mod integer_format_zero_suffix_tests {
    use super::*;
    #[test]
    fn precision_zero_and_fifteen_preserve_bigint_exactness() {
        assert_eq!(evaluate(&[Value::Integer(9_007_199_254_740_993), Value::Integer(15)]).unwrap(),
            Value::Text("9,007,199,254,740,993.000000000000000".into()));
        assert_eq!(evaluate(&[Value::Integer(-1), Value::Integer(1)]).unwrap(),
            Value::Text("-1.0".into()));
        assert!(evaluate(&[Value::Integer(1), Value::Integer(16)]).is_err());
    }
}

#[cfg(test)]
mod grouped_integer_fractional_suffix_tests {
    use super::*;

    #[test]
    fn keeps_full_bigint_precision_and_float_rounding() {
        assert_eq!(evaluate(&[Value::Integer(i64::MIN), Value::Integer(3)]).unwrap(),
            Value::Text("-9,223,372,036,854,775,808.000".into()));
        assert_eq!(evaluate(&[Value::Integer(1_234_567), Value::Integer(15)]).unwrap(),
            Value::Text("1,234,567.000000000000000".into()));
        assert_eq!(evaluate(&[Value::Integer(1_234_567), Value::Integer(0)]).unwrap(),
            Value::Text("1,234,567".into()));
        assert_eq!(evaluate(&[Value::Float(1234.5), Value::Integer(2)]).unwrap(),
            Value::Text("1,234.50".into()));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod integer_stack_format_tests {
    use super::*;

    #[test]
    fn formats_signed_boundaries_and_group_separator_positions() {
        for (number, expected) in [
            (i64::MIN, "-9,223,372,036,854,775,808"),
            (i64::MAX, "9,223,372,036,854,775,807"),
            (-1_234_567, "-1,234,567"),
            (0, "0"),
            (9, "9"),
            (999, "999"),
            (1000, "1,000"),
            (1_000_000, "1,000,000"),
        ] {
            for precision in [0, 1, 15] {
                let fractional = if precision == 0 {
                    String::new()
                } else {
                    format!(".{}", "0".repeat(precision as usize))
                };
                assert_eq!(
                    evaluate(&[
                        Value::Integer(number),
                        Value::Integer(precision),
                    ]).unwrap(),
                    Value::Text(format!("{expected}{fractional}")),
                );
            }
        }
        assert_eq!(
            evaluate(&[Value::Float(1234.5), Value::Integer(1)]).unwrap(),
            Value::Text("1,234.5".into()),
        );
    }
}
