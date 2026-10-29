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
pub(super) const FUNCTIONS: &[Function] = &[
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
