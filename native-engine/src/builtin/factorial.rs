use super::Function;
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
    let mut product = 1_i64;
    for current in 2..=value {
        product = product.checked_mul(current)
            .ok_or_else(|| Error::Execution("FACTORIAL overflow".into()))?;
    }
    Ok(Value::Integer(product))
}

fn lcm_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("LCM requires integer inputs".into())) }
}
fn lcm(args: &[Value]) -> Result<Value> {
    let Some(a) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(b) = super::integer(&args[1])? else { return Ok(Value::Null); };
    if a == 0 || b == 0 { return Ok(Value::Integer(0)); }
    let a = u128::from(a.unsigned_abs());
    let b = u128::from(b.unsigned_abs());
    let (mut x, mut y) = (a, b);
    while y != 0 { (x, y) = (y, x % y); }
    let multiple = (a / x).checked_mul(b)
        .ok_or_else(|| Error::Execution("LCM integer overflow".into()))?;
    i64::try_from(multiple).map(Value::Integer)
        .map_err(|_| Error::Execution("LCM result exceeds BIGINT range".into()))
}

pub(super) const FUNCTIONS: &[Function] = &[
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
