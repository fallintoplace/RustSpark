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
    let value = match args[0] {
        Value::Integer(n) => n as f64,
        Value::Float(n) if n.is_finite() => n,
        Value::Float(_) => return Err(Error::Execution("FORMAT_NUMBER requires finite number".into())),
        Value::Null => return Ok(Value::Null),
        _ => return Err(Error::Type("FORMAT_NUMBER requires a number".into())),
    };
    let Some(digits) = super::integer(&args[1])? else { return Ok(Value::Null) };
    if digits < 0 { return Ok(Value::Null) }
    let digits = usize::try_from(digits)
        .map_err(|_| Error::Execution("FORMAT_NUMBER precision overflow".into()))?;
    if digits > 15 { return Err(Error::Execution("FORMAT_NUMBER supports at most 15 decimal places".into())); }
    let formatted = format!("{value:.digits$}");
    let (negative, unsigned) = formatted.strip_prefix('-')
        .map_or(("", formatted.as_str()), |rest| ("-", rest));
    let (whole, fractional) = unsigned.split_once('.')
        .map_or((unsigned, None), |(whole, fractional)| (whole, Some(fractional)));
    let mut result = String::with_capacity(formatted.len().saturating_add(whole.len() / 3));
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
}
