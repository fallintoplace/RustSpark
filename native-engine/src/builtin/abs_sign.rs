use super::{float_type, numeric, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn abs_type(types: &[DataType]) -> Result<DataType> {
    numeric(types[0])?;
    Ok(types[0])
}
fn abs(args: &[Value]) -> Result<Value> {
    match args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => n.checked_abs().map(Value::Integer)
            .ok_or_else(|| Error::Execution("ABS integer overflow".into())),
        Value::Float(n) => super::finite(n.abs()),
        _ => Err(Error::Type("ABS expects numeric value".into())),
    }
}
fn sign(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Integer(value) => Ok(Value::Float(value.signum() as f64)),
        Value::Null => Ok(Value::Null),
        Value::Float(value) => {
            if !value.is_finite() { return Err(Error::Execution("SIGN received nonfinite value".into())); }
            // Signum preserves the sign bit of IEEE-754 negative zero.
            super::finite(if *value == 0.0 { *value } else { value.signum() })
        }
        _ => Err(Error::Type("SIGN expects numeric value".into())),
    }
}

fn is_even_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Boolean) }
    else { Err(Error::Type("IS_EVEN expects integer input".into())) }
}
fn is_even(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Boolean(number & 1 == 0))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "is_even", min_args: 1, max_args: 1, result_type: is_even_type, call: is_even },

    Function { name: "abs", min_args: 1, max_args: 1, result_type: abs_type, call: abs },
    Function { name: "sign", min_args: 1, max_args: 1, result_type: float_type, call: sign },
    Function { name: "signum", min_args: 1, max_args: 1, result_type: float_type, call: sign },
];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn abs_checks_overflow_and_sign_preserves_direction() {
        assert_eq!(abs(&[Value::Integer(-12)]).unwrap(), Value::Integer(12));
        assert!(abs(&[Value::Integer(i64::MIN)]).is_err());
        assert_eq!(sign(&[Value::Integer(-12)]).unwrap(), Value::Float(-1.0));
        assert_eq!(sign(&[Value::Null]).unwrap(), Value::Null);
        assert_eq!(sign(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert_eq!(sign(&[Value::Integer(i64::MIN)]).unwrap(), Value::Float(-1.0));
        assert_eq!(sign(&[Value::Integer(i64::MAX)]).unwrap(), Value::Float(1.0));
        assert_eq!(sign(&[Value::Float(-0.0)]).unwrap(), Value::Float(0.0));
        assert!(sign(&[Value::Float(f64::NAN)]).is_err());
    }
}

#[cfg(test)]
mod signum_alias_tests {
    #[test]
    fn spark_signum_is_registered() {
        assert!(crate::builtin::supports("signum"));
    }
}

#[cfg(test)]
mod is_even_extension_tests {
    use super::*;
    #[test]
    fn checks_numeric_extremes_and_nulls() {
        assert!(crate::builtin::supports("is_even"));
        assert_eq!(is_even(&[Value::Integer(-2)]).unwrap(), Value::Boolean(true));
        assert_eq!(is_even(&[Value::Integer(i64::MAX)]).unwrap(), Value::Boolean(false));
        assert_eq!(is_even(&[Value::Integer(i64::MIN)]).unwrap(), Value::Boolean(true));
        assert_eq!(is_even(&[Value::Integer(0)]).unwrap(), Value::Boolean(true));
        assert_eq!(is_even(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod signed_zero_signum_tests {
    use super::*;
    #[test]
    fn float_sign_preserves_negative_zero() {
        let Value::Float(negative) = sign(&[Value::Float(-0.0)]).unwrap() else {
            panic!("expected float");
        };
        assert_eq!(negative.to_bits(), (-0.0_f64).to_bits());
        let Value::Float(positive) = sign(&[Value::Float(0.0)]).unwrap() else {
            panic!("expected float");
        };
        assert_eq!(positive.to_bits(), 0.0_f64.to_bits());
        assert_eq!(sign(&[Value::Float(-3.0)]).unwrap(), Value::Float(-1.0));
        assert_eq!(sign(&[Value::Null]).unwrap(), Value::Null);
    }
}
