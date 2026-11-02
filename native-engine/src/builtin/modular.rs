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
            let divisor = i128::from(*b);
            let dividend = i128::from(*a);
            let result = if divisor > 0 { dividend.rem_euclid(divisor) } else { dividend % divisor };
            return i64::try_from(result).map(Value::Integer)
                .map_err(|_| Error::Execution("PMOD integer overflow".into()));
        }
        return a.checked_rem(*b).map(Value::Integer)
            .ok_or_else(|| Error::Execution("MOD integer overflow".into()));
    }
    let a = super::number(&args[0])?.expect("NULL handled");
    let b = super::number(&args[1])?.expect("NULL handled");
    if b == 0.0 { return Err(Error::Execution("MOD divisor must not be zero".into())); }
    super::finite(if positive && b > 0.0 { a.rem_euclid(b) } else { a % b })
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

fn try_mod(args: &[Value]) -> Result<Value> {
    if args.iter().any(Value::is_null) { return Ok(Value::Null); }
    if let (Value::Integer(left), Value::Integer(right)) = (&args[0], &args[1]) {
        return Ok(left.checked_rem(*right)
            .map(Value::Integer).unwrap_or(Value::Null));
    }
    let Some(left) = super::number(&args[0])? else { return Ok(Value::Null); };
    let Some(right) = super::number(&args[1])? else { return Ok(Value::Null); };
    if right == 0.0 { return Ok(Value::Null); }
    let result = left % right;
    Ok(if result.is_finite() { Value::Float(result) } else { Value::Null })
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "gcd", min_args: 2, max_args: 2, result_type: gcd_type, call: gcd },
    Function { name: "mod", min_args: 2, max_args: 2, result_type: output, call: modulo },
    Function { name: "try_mod", min_args: 2, max_args: 2, result_type: output, call: try_mod },
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
        assert_eq!(pmod(&[Value::Integer(-10), Value::Integer(-3)]).unwrap(), Value::Integer(-1));
        assert_eq!(pmod(&[Value::Integer(10), Value::Integer(-3)]).unwrap(), Value::Integer(1));
        assert_eq!(pmod(&[Value::Integer(i64::MIN), Value::Integer(-1)]).unwrap(), Value::Integer(0));
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
        assert_eq!(gcd(&[Value::Integer(i64::MIN), Value::Integer(2)]).unwrap(), Value::Integer(2));
        assert!(gcd_type(&[DataType::Text, DataType::Integer]).is_err());
        assert_eq!(gcd(&[Value::Null, Value::Integer(5)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod spark_try_mod_native_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};
    #[test]
    fn invalid_remainders_are_nullable_instead_of_errors() {
        assert_eq!(try_mod(&[Value::Integer(7), Value::Integer(3)]).unwrap(), Value::Integer(1));
        assert_eq!(try_mod(&[Value::Integer(-7), Value::Integer(3)]).unwrap(), Value::Integer(-1));
        assert_eq!(try_mod(&[Value::Integer(1), Value::Integer(0)]).unwrap(), Value::Null);
        assert_eq!(try_mod(&[Value::Integer(i64::MIN), Value::Integer(-1)]).unwrap(), Value::Null);
        assert_eq!(try_mod(&[Value::Float(3.5), Value::Integer(2)]).unwrap(), Value::Float(1.5));
        assert_eq!(try_mod(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text, DataType::Integer]).is_err());
    }
    #[test]
    fn executes_through_native_sql() {
        let query = sql::parse_sql("SELECT TRY_MOD(7, 3), TRY_MOD(1, 0)").unwrap();
        assert_eq!(executor::execute(&query, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1), Value::Null]]);
    }
}
