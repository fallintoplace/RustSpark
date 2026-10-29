use super::{float_type, Function};
use crate::{scalar::Value, Result};
fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn exp(args: &[Value]) -> Result<Value> { apply(args, f64::exp) }
fn expm1(args: &[Value]) -> Result<Value> { apply(args, f64::exp_m1) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "exp", min_args: 1, max_args: 1, result_type: float_type, call: exp },
    Function { name: "expm1", min_args: 1, max_args: 1, result_type: float_type, call: expm1 },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exponential_math_is_finite() {
        assert_eq!(exp(&[Value::Integer(0)]).unwrap(), Value::Float(1.0));
        assert_eq!(expm1(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert!(exp(&[Value::Float(1000.0)]).is_err());
    }
}
