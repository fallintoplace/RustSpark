use super::{numeric, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    for &kind in types { numeric(kind)?; }
    if types.iter().all(|&kind| kind == DataType::Integer) {
        Ok(DataType::Integer)
    } else { Ok(DataType::Float) }
}
fn remainder(args: &[Value], positive: bool) -> Result<Value> {
    if args.iter().any(Value::is_null) { return Ok(Value::Null); }
    if let (Value::Integer(a), Value::Integer(b)) = (&args[0], &args[1]) {
        if *b == 0 { return Err(Error::Execution("MOD divisor must not be zero".into())); }
        if positive {
            let divisor = i128::from(*b).abs();
            let result = i128::from(*a).rem_euclid(divisor);
            return i64::try_from(result).map(Value::Integer)
                .map_err(|_| Error::Execution("PMOD integer overflow".into()));
        }
        return a.checked_rem(*b).map(Value::Integer)
            .ok_or_else(|| Error::Execution("MOD integer overflow".into()));
    }
    let a = super::number(&args[0])?.expect("NULL handled");
    let b = super::number(&args[1])?.expect("NULL handled");
    if b == 0.0 { return Err(Error::Execution("MOD divisor must not be zero".into())); }
    super::finite(if positive { a.rem_euclid(b) } else { a % b })
}
fn modulo(args: &[Value]) -> Result<Value> { remainder(args, false) }
fn pmod(args: &[Value]) -> Result<Value> { remainder(args, true) }

fn gcd_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("GCD requires integer inputs".into())) }
}
fn gcd(args: &[Value]) -> Result<Value> {
    let Some(a) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(b) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let mut a = a.unsigned_abs();
    let mut b = b.unsigned_abs();
    while b != 0 {
        (a, b) = (b, a % b);
    }
    i64::try_from(a).map(Value::Integer)
        .map_err(|_| Error::Execution("GCD result exceeds BIGINT range".into()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "gcd", min_args: 2, max_args: 2, result_type: gcd_type, call: gcd },
    Function { name: "mod", min_args: 2, max_args: 2, result_type: output, call: modulo },
    Function { name: "pmod", min_args: 2, max_args: 2, result_type: output, call: pmod },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn modulo_and_positive_remainder_handle_signed_inputs() {
        assert_eq!(modulo(&[Value::Integer(-10), Value::Integer(3)]).unwrap(), Value::Integer(-1));
        assert_eq!(pmod(&[Value::Integer(-10), Value::Integer(3)]).unwrap(), Value::Integer(2));
        assert!(modulo(&[Value::Integer(i64::MIN), Value::Integer(-1)]).is_err());
        assert!(pmod(&[Value::Integer(1), Value::Integer(0)]).is_err());
    }
}

#[cfg(test)]
mod gcd_extension_tests {
    use super::*;
    #[test]
    fn checks_integer_boundaries_and_nulls() {
        assert!(crate::builtin::supports("gcd"));
        assert_eq!(gcd(&[Value::Integer(-54), Value::Integer(24)]).unwrap(), Value::Integer(6));
        assert_eq!(gcd(&[Value::Integer(0), Value::Integer(0)]).unwrap(), Value::Integer(0));
        assert_eq!(gcd(&[Value::Integer(i64::MIN), Value::Integer(3)]).unwrap(), Value::Integer(1));
        assert!(gcd(&[Value::Integer(i64::MIN), Value::Integer(0)]).is_err());
        assert_eq!(gcd(&[Value::Null, Value::Integer(5)]).unwrap(), Value::Null);
    }
}
