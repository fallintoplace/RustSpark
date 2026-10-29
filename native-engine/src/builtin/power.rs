use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn power(args: &[Value]) -> Result<Value> {
    let Some(base) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(exponent) = super::number(&args[1])? else { return Ok(Value::Null) };
    let result = match &args[1] {
        Value::Integer(n) if base == -1.0 => if *n & 1 == 0 { 1.0 } else { -1.0 },
        Value::Integer(_) if base == 1.0 => 1.0,
        Value::Integer(n) => match i32::try_from(*n) {
            Ok(power) => base.powi(power),
            Err(_) => base.powf(exponent),
        },
        _ => base.powf(exponent),
    };
    super::finite(result)
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "power", min_args: 2, max_args: 2, result_type: float_type, call: power },
    Function { name: "pow", min_args: 2, max_args: 2, result_type: float_type, call: power },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pow_is_typed_and_rejects_unbounded_results() {
        assert_eq!(power(&[Value::Integer(2), Value::Integer(10)]).unwrap(), Value::Float(1024.0));
        assert_eq!(power(&[Value::Integer(2), Value::Integer(-3)]).unwrap(), Value::Float(0.125));
        assert_eq!(power(&[Value::Integer(-2), Value::Integer(5)]).unwrap(), Value::Float(-32.0));
        assert_eq!(power(&[Value::Integer(9), Value::Float(0.5)]).unwrap(), Value::Float(3.0));
        assert!(power(&[Value::Integer(0), Value::Integer(-1)]).is_err());
        assert_eq!(power(&[Value::Null, Value::Integer(9)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod exact_integer_exponent_tests {
    use super::*;
    #[test]
    fn very_large_integer_exponents_keep_negative_one_parity() {
        assert_eq!(power(&[Value::Integer(-1), Value::Integer(i64::MAX)]).unwrap(), Value::Float(-1.0));
        assert_eq!(power(&[Value::Integer(-1), Value::Integer(i64::MIN)]).unwrap(), Value::Float(1.0));
        assert_eq!(power(&[Value::Integer(-1), Value::Integer(9_007_199_254_740_993)]).unwrap(), Value::Float(-1.0));
        assert_eq!(power(&[Value::Integer(1), Value::Integer(i64::MAX)]).unwrap(), Value::Float(1.0));
        assert_eq!(power(&[Value::Null, Value::Integer(i64::MAX)]).unwrap(), Value::Null);
    }
}
