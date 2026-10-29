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
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    if !value.is_finite() { return Err(Error::Execution("SIGN received nonfinite value".into())); }
    super::finite(value.signum())
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "abs", min_args: 1, max_args: 1, result_type: abs_type, call: abs },
    Function { name: "sign", min_args: 1, max_args: 1, result_type: float_type, call: sign },
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
    }
}
