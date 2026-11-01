use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(&types[..1])?;
    if types.len() == 2 && types[1] != DataType::Integer {
        return Err(Error::Type("BROUND scale must be an integer".into()));
    }
    Ok(if types[0] == DataType::Integer { DataType::Integer } else { DataType::Float })
}
fn round_even(args: &[Value]) -> Result<Value> {
    let scale = if args.len() == 2 {
        let Some(scale) = super::integer(&args[1])? else { return Ok(Value::Null) };
        scale
    } else { 0 };
    if !(-15..=15).contains(&scale) {
        return Err(Error::Execution("BROUND scale must be within [-15,15]".into()));
    }
    if let Value::Integer(value) = args[0] {
        if scale >= 0 { return Ok(Value::Integer(value)); }
        let factor = 10_i128.pow((-scale) as u32);
        let value = i128::from(value);
        let magnitude = value.abs();
        let quotient = magnitude / factor;
        let remainder = magnitude % factor;
        let increment = remainder > factor / 2 || (remainder == factor / 2 && quotient % 2 == 1);
        let rounded = (quotient + i128::from(increment)) * factor * value.signum();
        return i64::try_from(rounded).map(Value::Integer)
            .map_err(|_| Error::Execution("BROUND integer overflow".into()));
    }
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    let factor = 10_f64.powi(scale.unsigned_abs() as i32);
    let result = if scale >= 0 {
        if x.abs() > f64::MAX / factor { return super::finite(x); }
        (x * factor).round_ties_even() / factor
    } else {
        // Large doubles are already exact multiples at this decimal
        // scale. Avoid a divide/multiply round trip changing their bits.
        if x.abs() >= factor * (1u64 << 53) as f64 {
            return super::finite(x);
        }
        (x / factor).round_ties_even() * factor
    };
    super::finite(result)
}
fn rint(args: &[Value]) -> Result<Value> {
    match super::number(&args[0])? {
        Some(value) => super::finite(value.round_ties_even()),
        None => Ok(Value::Null),
    }
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bround", min_args: 1, max_args: 2, result_type: output, call: round_even },
    Function { name: "rint", min_args: 1, max_args: 1, result_type: float_type, call: rint },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ties_to_even_differs_from_round_away_from_zero() {
        assert_eq!(rint(&[Value::Float(2.5)]).unwrap(), Value::Float(2.0));
        assert_eq!(rint(&[Value::Float(3.5)]).unwrap(), Value::Float(4.0));
        assert_eq!(round_even(&[Value::Float(2.5), Value::Integer(0)])
            .unwrap(), Value::Float(2.0));
        assert_eq!(round_even(&[Value::Float(-3.5), Value::Integer(0)])
            .unwrap(), Value::Float(-4.0));
        assert_eq!(round_even(&[Value::Float(125.0), Value::Integer(-1)]).unwrap(), Value::Float(120.0));
        assert_eq!(round_even(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bigint_bround_exactness_tests {
    use super::*;
    #[test]
    fn round_even_preserves_bigint_and_rint_remains_float() {
        assert_eq!(output(&[DataType::Integer]).unwrap(), DataType::Integer);
        assert_eq!(round_even(&[Value::Integer(i64::MAX)]).unwrap(), Value::Integer(i64::MAX));
        assert_eq!(round_even(&[Value::Integer(9_007_199_254_740_993), Value::Integer(2)]).unwrap(),
            Value::Integer(9_007_199_254_740_993));
        assert_eq!(round_even(&[Value::Integer(125), Value::Integer(-1)]).unwrap(), Value::Integer(120));
        assert_eq!(round_even(&[Value::Integer(135), Value::Integer(-1)]).unwrap(), Value::Integer(140));
        assert_eq!(round_even(&[Value::Integer(-125), Value::Integer(-1)]).unwrap(), Value::Integer(-120));
        assert_eq!(rint(&[Value::Integer(3)]).unwrap(), Value::Float(3.0));
        assert_eq!(rint(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod large_finite_bround_float_tests {
    use super::*;
    #[test]
    fn large_finite_values_skip_overflowing_decimal_scaling() {
        assert_eq!(round_even(&[Value::Float(f64::MAX), Value::Integer(2)]).unwrap(), Value::Float(f64::MAX));
        assert_eq!(round_even(&[Value::Float(-f64::MAX), Value::Integer(15)]).unwrap(), Value::Float(-f64::MAX));
        assert_eq!(round_even(&[Value::Float(2.5)]).unwrap(), Value::Float(2.0));
        assert_eq!(round_even(&[Value::Float(3.5)]).unwrap(), Value::Float(4.0));
    }
}

#[cfg(test)]
mod inexact_large_negative_bround_regressions {
    use super::*;

    #[test]
    fn large_floats_do_not_shift_by_one_ulp_at_negative_scales() {
        for (value, scale) in [
            (-3.3036582842864976e239, -1),
            (2.4560805462934964e260, -3),
            (9.234641875553306e113, -8),
            (f64::MAX, -15),
            (-f64::MAX, -15),
        ] {
            let rounded = round_even(&[Value::Float(value), Value::Integer(scale)]).unwrap();
            assert_eq!(rounded, Value::Float(value), "value={value}, scale={scale}");
        }
        assert_eq!(round_even(&[Value::Float(125.0), Value::Integer(-1)]).unwrap(),
            Value::Float(120.0));
        assert_eq!(round_even(&[Value::Float(135.0), Value::Integer(-1)]).unwrap(),
            Value::Float(140.0));
    }
}
