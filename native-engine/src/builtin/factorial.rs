use super::Function;
const FACTORIALS: [i64; 21] = [1, 1, 2, 6, 24, 120, 720, 5040, 40320, 362880, 3628800, 39916800, 479001600, 6227020800, 87178291200, 1307674368000, 20922789888000, 355687428096000, 6402373705728000, 121645100408832000, 2432902008176640000];
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("FACTORIAL requires integer input".into())) }
}
fn factorial(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    if !(0..=20).contains(&value) {
        return Ok(Value::Null);
    }
    Ok(Value::Integer(FACTORIALS[value as usize]))
}

fn gcd_magnitude(mut a: u64, mut b: u64) -> u64 {
    while b != 0 { (a, b) = (b, a % b); }
    a
}

fn lcm_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("LCM requires integer inputs".into())) }
}
fn lcm(args: &[Value]) -> Result<Value> {
    let Some(a) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(b) = super::integer(&args[1])? else { return Ok(Value::Null); };
    if a == 0 || b == 0 { return Ok(Value::Integer(0)); }
    let a = a.unsigned_abs();
    let b = b.unsigned_abs();
    let divisor = u128::from(gcd_magnitude(a, b));
    let multiple = (u128::from(a) / divisor).checked_mul(u128::from(b))
        .ok_or_else(|| Error::Execution("LCM integer overflow".into()))?;
    i64::try_from(multiple).map(Value::Integer)
        .map_err(|_| Error::Execution("LCM result exceeds BIGINT range".into()))
}

fn gcd_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("GCD requires integer inputs".into())) }
}
fn gcd(args: &[Value]) -> Result<Value> {
    let Some(a) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(b) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let divisor = gcd_magnitude(a.unsigned_abs(), b.unsigned_abs());
    i64::try_from(divisor).map(Value::Integer)
        .map_err(|_| Error::Execution("GCD exceeds BIGINT range".into()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "gcd", min_args: 2, max_args: 2, result_type: gcd_type, call: gcd },
    Function { name: "lcm", min_args: 2, max_args: 2, result_type: lcm_type, call: lcm },
    Function { name: "factorial", min_args: 1, max_args: 1, result_type: output, call: factorial },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn factorial_bounds_and_nulls() {
        assert_eq!(factorial(&[Value::Integer(0)]).unwrap(), Value::Integer(1));
        assert_eq!(factorial(&[Value::Integer(5)]).unwrap(), Value::Integer(120));
        assert_eq!(factorial(&[Value::Integer(20)]).unwrap(),
            Value::Integer(2_432_902_008_176_640_000));
        for n in 0..=20 {
            let expected = (1_i64..=n).product::<i64>();
            assert_eq!(factorial(&[Value::Integer(n)]).unwrap(), Value::Integer(expected));
        }
        assert_eq!(factorial(&[Value::Integer(21)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Integer(i64::MAX)]).unwrap(), Value::Null);
        assert_eq!(factorial(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod lcm_extension_tests {
    use super::*;
    #[test]
    fn checks_integer_boundaries_and_nulls() {
        assert!(crate::builtin::supports("lcm"));
        assert_eq!(lcm(&[Value::Integer(6), Value::Integer(8)]).unwrap(), Value::Integer(24));
        assert_eq!(lcm(&[Value::Integer(-6), Value::Integer(8)]).unwrap(), Value::Integer(24));
        assert_eq!(lcm(&[Value::Integer(0), Value::Integer(i64::MIN)]).unwrap(), Value::Integer(0));
        assert!(lcm(&[Value::Integer(i64::MAX), Value::Integer(2)]).is_err());
        assert_eq!(lcm(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod gcd_native_feature_tests {
    use super::*;
    #[test]
    fn gcd_handles_signs_zero_nulls_and_overflow() {
        assert!(crate::builtin::supports("gcd"));
        assert_eq!(gcd(&[Value::Integer(-42), Value::Integer(30)]).unwrap(), Value::Integer(6));
        assert_eq!(gcd(&[Value::Integer(0), Value::Integer(0)]).unwrap(), Value::Integer(0));
        assert_eq!(gcd(&[Value::Integer(i64::MIN), Value::Integer(2)]).unwrap(), Value::Integer(2));
        assert!(gcd(&[Value::Integer(i64::MIN), Value::Integer(0)]).is_err());
        assert_eq!(gcd(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
        assert!(gcd_type(&[DataType::Text, DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod gcd_shared_math_tests {
    use super::*;

    #[test]
    fn preserves_gcd_lcm_signs_and_overflow_errors() {
        assert_eq!(gcd(&[Value::Integer(-12), Value::Integer(18)]).unwrap(),
            Value::Integer(6));
        assert_eq!(lcm(&[Value::Integer(-12), Value::Integer(18)]).unwrap(),
            Value::Integer(36));
        assert_eq!(gcd(&[Value::Integer(0), Value::Integer(0)]).unwrap(),
            Value::Integer(0));
        assert!(gcd(&[Value::Integer(i64::MIN), Value::Integer(0)]).is_err());
        assert!(lcm(&[Value::Integer(i64::MAX), Value::Integer(2)]).is_err());
    }
}
