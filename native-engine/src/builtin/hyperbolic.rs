use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn sinh(args: &[Value]) -> Result<Value> { apply(args, f64::sinh) }
fn cosh(args: &[Value]) -> Result<Value> { apply(args, f64::cosh) }
fn tanh(args: &[Value]) -> Result<Value> { apply(args, f64::tanh) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sinh", min_args: 1, max_args: 1, result_type: float_type, call: sinh },
    Function { name: "cosh", min_args: 1, max_args: 1, result_type: float_type, call: cosh },
    Function { name: "tanh", min_args: 1, max_args: 1, result_type: float_type, call: tanh },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hyperbolic_math_is_bounded() {
        assert_eq!(sinh(&[Value::Float(0.0)]).unwrap(), Value::Float(0.0));
        assert_eq!(cosh(&[Value::Float(0.0)]).unwrap(), Value::Float(1.0));
        assert_eq!(tanh(&[Value::Null]).unwrap(), Value::Null);
        assert!(cosh(&[Value::Float(1000.0)]).is_err());
    }
}
