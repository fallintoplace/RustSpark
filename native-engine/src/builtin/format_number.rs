use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) && types[1] == DataType::Integer {
        Ok(DataType::Text)
    } else {
        Err(Error::Type("FORMAT_NUMBER requires number and integer precision".into()))
    }
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
    let formatted = match args[0] {
        // Casting BIGINT to f64 silently rounds values above 2^53.
        Value::Integer(value) => value.to_string(),
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
    } else if matches!(args[0], Value::Integer(_)) && digits > 0 {
        const ZEROS: &str = "000000000000000";
        result.push('.');
        result.push_str(&ZEROS[..digits]);
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
