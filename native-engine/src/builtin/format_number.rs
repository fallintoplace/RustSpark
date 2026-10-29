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
    let mut reversed = String::new();
    for (index, ch) in whole.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 { reversed.push(','); }
        reversed.push(ch);
    }
    let grouped: String = reversed.chars().rev().collect();
    let suffix = fractional.map_or(String::new(), |part| format!(".{part}"));
    Ok(Value::Text(format!("{negative}{grouped}{suffix}")))
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
