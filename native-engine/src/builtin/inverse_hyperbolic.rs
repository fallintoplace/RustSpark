use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn asinh(args: &[Value]) -> Result<Value> { apply(args, f64::asinh) }
fn acosh(args: &[Value]) -> Result<Value> { apply(args, f64::acosh) }
fn atanh(args: &[Value]) -> Result<Value> { apply(args, f64::atanh) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "asinh", min_args: 1, max_args: 1, result_type: float_type, call: asinh },
    Function { name: "acosh", min_args: 1, max_args: 1, result_type: float_type, call: acosh },
    Function { name: "atanh", min_args: 1, max_args: 1, result_type: float_type, call: atanh },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inverse_hyperbolic_domains_are_checked() {
        assert_eq!(asinh(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert_eq!(acosh(&[Value::Integer(1)]).unwrap(), Value::Float(0.0));
        assert!(acosh(&[Value::Integer(0)]).is_err());
        assert!(atanh(&[Value::Integer(1)]).is_err());
    }
}
