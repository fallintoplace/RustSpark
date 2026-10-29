use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(&types[..1])?;
    if types.len() == 2 && types[1] != DataType::Integer {
        return Err(Error::Type("ROUND precision must be an integer".into()));
    }
    Ok(DataType::Float)
}
fn round(args: &[Value]) -> Result<Value> {
    let Some(number) = super::number(&args[0])? else { return Ok(Value::Null) };
    let digits = if args.len() == 2 {
        let Some(digits) = super::integer(&args[1])? else { return Ok(Value::Null) };
        digits
    } else { 0 };
    if !(-15..=15).contains(&digits) {
        return Err(Error::Execution("ROUND precision outside supported [-15,15] range".into()));
    }
    let scale = 10_f64.powi(digits.unsigned_abs() as i32);
    let result = if digits >= 0 {
        (number * scale).round() / scale
    } else {
        (number / scale).round() * scale
    };
    super::finite(result)
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "round", min_args: 1, max_args: 2, result_type: output, call: round,
}];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_positive_and_negative_precision() {
        assert_eq!(round(&[Value::Float(1.235), Value::Integer(2)]).unwrap(), Value::Float(1.24));
        assert_eq!(round(&[Value::Float(149.0), Value::Integer(-2)]).unwrap(), Value::Float(100.0));
        assert!(round(&[Value::Float(1.0), Value::Integer(100)]).is_err());
    }
}
