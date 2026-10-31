use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(&types[..1])?;
    if types.len() == 2 {
        if types[1] != DataType::Integer {
            return Err(Error::Type("CEIL/FLOOR scale must be integer".into()));
        }
        return Ok(DataType::Float);
    }
    Ok(DataType::Integer)
}
fn checked_integer(value: f64) -> Result<Value> {
    if !value.is_finite() || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value) {
        return Err(Error::Execution("rounded SQL value is outside signed 64-bit range".into()));
    }
    Ok(Value::Integer(value as i64))
}
fn scale_round(args: &[Value], round_up: bool) -> Result<Value> {
    let Some(number) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(scale) = super::integer(&args[1])? else { return Ok(Value::Null) };
    if !(-15..=15).contains(&scale) {
        return Err(Error::Execution("CEIL/FLOOR scale outside supported [-15, 15] range".into()));
    }
    let factor = 10_f64.powi(scale.unsigned_abs() as i32);
    let round = |value: f64| if round_up { value.ceil() } else { value.floor() };
    let result = if scale >= 0 {
        // A sub-ULP decimal scale cannot change a large finite number.
        // Avoid overflowing the intermediate multiplication to infinity.
        if number.abs() > f64::MAX / factor { return super::finite(number); }
        round(number * factor) / factor
    } else {
        round(number / factor) * factor
    };
    super::finite(result)
}

fn floor(args: &[Value]) -> Result<Value> {
    if args.len() == 2 { return scale_round(args, false); }
    if let Value::Integer(value) = args[0] { return Ok(Value::Integer(value)); }
    match super::number(&args[0])? {
        Some(v) => checked_integer(v.floor()),
        None => Ok(Value::Null),
    }
}
fn ceil(args: &[Value]) -> Result<Value> {
    if args.len() == 2 { return scale_round(args, true); }
    if let Value::Integer(value) = args[0] { return Ok(Value::Integer(value)); }
    match super::number(&args[0])? {
        Some(v) => checked_integer(v.ceil()),
        None => Ok(Value::Null),
    }
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "floor", min_args: 1, max_args: 2, result_type: output, call: floor },
    Function { name: "ceil", min_args: 1, max_args: 2, result_type: output, call: ceil },
    Function { name: "ceiling", min_args: 1, max_args: 2, result_type: output, call: ceil },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn floor_and_ceil_are_signed_and_checked() {
        assert_eq!(floor(&[Value::Float(-1.2)]).unwrap(), Value::Integer(-2));
        assert_eq!(ceil(&[Value::Float(-1.2)]).unwrap(), Value::Integer(-1));
        assert!(ceil(&[Value::Float(f64::MAX)]).is_err());
        assert_eq!(floor(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(ceil(&[Value::Float(3.1411), Value::Integer(3)]).unwrap(), Value::Float(3.142));
        assert_eq!(ceil(&[Value::Float(3.1411), Value::Integer(-3)]).unwrap(), Value::Float(1000.0));
        assert_eq!(ceil(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
        assert!(ceil(&[Value::Float(3.14), Value::Integer(100)]).is_err());
        assert_eq!(output(&[DataType::Float, DataType::Integer]).unwrap(), DataType::Float);
        assert_eq!(floor(&[Value::Float(3.1411), Value::Integer(3)]).unwrap(), Value::Float(3.141));
        assert_eq!(floor(&[Value::Float(3.1411), Value::Integer(-3)]).unwrap(), Value::Float(0.0));
        assert_eq!(floor(&[Value::Float(-3.1411), Value::Integer(-3)]).unwrap(), Value::Float(-1000.0));
        assert!(floor(&[Value::Float(1.0), Value::Integer(30)]).is_err());
        assert_eq!(floor(&[Value::Integer(i64::MAX)]).unwrap(), Value::Integer(i64::MAX));
        assert_eq!(ceil(&[Value::Integer(i64::MIN)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(ceil(&[Value::Integer(9_007_199_254_740_993)]).unwrap(),
            Value::Integer(9_007_199_254_740_993));
    }
}

#[cfg(test)]
mod large_finite_scaled_rounding_tests {
    use super::*;

    #[test]
    fn positive_precision_does_not_overflow_finite_values() {
        for value in [f64::MAX, -f64::MAX] {
            let input = [Value::Float(value), Value::Integer(15)];
            assert_eq!(floor(&input).unwrap(), Value::Float(value));
            assert_eq!(ceil(&input).unwrap(), Value::Float(value));
        }
        assert_eq!(
            ceil(&[Value::Float(3.1411), Value::Integer(3)]).unwrap(),
            Value::Float(3.142),
        );
    }
}
