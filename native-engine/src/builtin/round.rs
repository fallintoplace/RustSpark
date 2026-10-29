use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(&types[..1])?;
    if types.len() == 2 && types[1] != DataType::Integer {
        return Err(Error::Type("ROUND precision must be an integer".into()));
    }
    Ok(if types[0] == DataType::Integer { DataType::Integer } else { DataType::Float })
}
fn round(args: &[Value]) -> Result<Value> {
    let digits = if args.len() == 2 {
        let Some(digits) = super::integer(&args[1])? else { return Ok(Value::Null) };
        digits
    } else { 0 };
    if !(-15..=15).contains(&digits) {
        return Err(Error::Execution("ROUND precision outside supported [-15,15] range".into()));
    }
    if let Value::Integer(value) = args[0] {
        if digits >= 0 { return Ok(Value::Integer(value)); }
        let unit = 10_i128.pow((-digits) as u32);
        let value = i128::from(value);
        let magnitude = value.abs();
        let rounded = ((magnitude + unit / 2) / unit) * unit * value.signum();
        return i64::try_from(rounded).map(Value::Integer)
            .map_err(|_| Error::Execution("ROUND integer overflow".into()));
    }
    let Some(number) = super::number(&args[0])? else { return Ok(Value::Null) };
    let scale = 10_f64.powi(digits.unsigned_abs() as i32);
    let result = if digits >= 0 {
        (number * scale).round() / scale
    } else {
        (number / scale).round() * scale
    };
    super::finite(result)
}
fn is_odd_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Boolean) }
    else { Err(Error::Type("IS_ODD expects integer input".into())) }
}
fn is_odd(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Boolean(number & 1 != 0))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "is_odd", min_args: 1, max_args: 1, result_type: is_odd_type, call: is_odd },
Function {
    name: "round", min_args: 1, max_args: 2, result_type: output, call: round,
}];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_positive_and_negative_precision() {
        assert_eq!(round(&[Value::Float(1.235), Value::Integer(2)]).unwrap(), Value::Float(1.24));
        assert_eq!(round(&[Value::Float(149.0), Value::Integer(-2)]).unwrap(), Value::Float(100.0));
        assert_eq!(round(&[Value::Float(2.5)]).unwrap(), Value::Float(3.0));
        assert_eq!(round(&[Value::Float(-2.5), Value::Integer(0)]).unwrap(),
            Value::Float(-3.0));
        assert!(round(&[Value::Float(1.0), Value::Integer(100)]).is_err());
    }
}

#[cfg(test)]
mod is_odd_extension_tests {
    use super::*;
    #[test]
    fn checks_numeric_extremes_and_nulls() {
        assert!(crate::builtin::supports("is_odd"));
        assert_eq!(is_odd(&[Value::Integer(-3)]).unwrap(), Value::Boolean(true));
        assert_eq!(is_odd(&[Value::Integer(i64::MAX)]).unwrap(), Value::Boolean(true));
        assert_eq!(is_odd(&[Value::Integer(i64::MIN)]).unwrap(), Value::Boolean(false));
        assert_eq!(is_odd(&[Value::Integer(0)]).unwrap(), Value::Boolean(false));
        assert_eq!(is_odd(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bigint_round_exactness_tests {
    use super::*;
    #[test]
    fn round_preserves_signed_bigints_and_scale() {
        assert_eq!(output(&[DataType::Integer]).unwrap(), DataType::Integer);
        assert_eq!(output(&[DataType::Float]).unwrap(), DataType::Float);
        assert_eq!(round(&[Value::Integer(i64::MAX)]).unwrap(), Value::Integer(i64::MAX));
        assert_eq!(round(&[Value::Integer(9_007_199_254_740_993), Value::Integer(3)]).unwrap(), Value::Integer(9_007_199_254_740_993));
        assert_eq!(round(&[Value::Integer(125), Value::Integer(-1)]).unwrap(), Value::Integer(130));
        assert_eq!(round(&[Value::Integer(-125), Value::Integer(-1)]).unwrap(), Value::Integer(-130));
        assert_eq!(round(&[Value::Integer(-149), Value::Integer(-2)]).unwrap(), Value::Integer(-100));
        assert_eq!(round(&[Value::Null]).unwrap(), Value::Null);
    }
}
