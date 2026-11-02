use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn integer_power(base: f64, exponent: i64, approximate_exponent: f64) -> f64 {
    // Float conversion loses parity for integer exponents beyond 2^53.
    if base == -1.0 { return if exponent & 1 == 0 { 1.0 } else { -1.0 }; }
    if base == 1.0 { return 1.0; }
    match i32::try_from(exponent) {
        Ok(power) => base.powi(power),
        Err(_) => base.powf(approximate_exponent),
    }
}

fn power(args: &[Value]) -> Result<Value> {
    let Some(base) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(exponent) = super::number(&args[1])? else { return Ok(Value::Null) };
    let result = match &args[1] {
        Value::Integer(integer_exponent) => integer_power(base, *integer_exponent, exponent),
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

#[cfg(test)]
mod isolated_integer_exponent_tests {
    use super::*;

    #[test]
    fn integer_exponents_keep_exact_parity_and_powi_results() {
        for exponent in [-5_i64, -3, 0, 1, 5, 10, 100] {
            assert_eq!(integer_power(2.0, exponent, exponent as f64),
                2.0_f64.powi(exponent as i32));
        }
        for exponent in [i64::MIN, i64::MAX, 9_007_199_254_740_993] {
            let expected = if exponent & 1 == 0 { 1.0 } else { -1.0 };
            assert_eq!(integer_power(-1.0, exponent, exponent as f64), expected);
            assert_eq!(integer_power(1.0, exponent, exponent as f64), 1.0);
            assert_eq!(power(&[
                Value::Integer(-1), Value::Integer(exponent),
            ]).unwrap(), Value::Float(expected));
        }
        assert_eq!(power(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
    }
}
