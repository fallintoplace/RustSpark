use super::{float_type, Function};
use crate::{scalar::Value, Error, Result};
fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(x))
}
fn asin(args: &[Value]) -> Result<Value> { apply(args, f64::asin) }
fn acos(args: &[Value]) -> Result<Value> { apply(args, f64::acos) }
fn atan(args: &[Value]) -> Result<Value> { apply(args, f64::atan) }
fn atan2(args: &[Value]) -> Result<Value> {
    let Some(y) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(x) = super::number(&args[1])? else { return Ok(Value::Null) };
    if !x.is_finite() || !y.is_finite() {
        return Err(Error::Execution("ATAN2 requires finite inputs".into()));
    }
    super::finite(y.atan2(x))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "asin", min_args: 1, max_args: 1, result_type: float_type, call: asin },
    Function { name: "acos", min_args: 1, max_args: 1, result_type: float_type, call: acos },
    Function { name: "atan", min_args: 1, max_args: 1, result_type: float_type, call: atan },
    Function { name: "atan2", min_args: 2, max_args: 2, result_type: float_type, call: atan2 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inverse_trig_checks_the_domain() {
        assert_eq!(acos(&[Value::Integer(1)]).unwrap(), Value::Float(0.0));
        assert!(asin(&[Value::Integer(2)]).is_err());
        assert_eq!(atan(&[Value::Null]).unwrap(), Value::Null);
        let Value::Float(angle) = atan2(&[Value::Integer(0), Value::Integer(-1)]).unwrap()
            else { panic!("ATAN2 should return a float"); };
        assert!((angle - std::f64::consts::PI).abs() < 1e-12);
    }
}
