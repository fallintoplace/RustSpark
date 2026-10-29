use super::{float_type, Function};
use crate::{scalar::Value, Result};
fn apply(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(x))
}
fn sin(args: &[Value]) -> Result<Value> { apply(args, f64::sin) }
fn cos(args: &[Value]) -> Result<Value> { apply(args, f64::cos) }
fn tan(args: &[Value]) -> Result<Value> { apply(args, f64::tan) }
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sin", min_args: 1, max_args: 1, result_type: float_type, call: sin },
    Function { name: "cos", min_args: 1, max_args: 1, result_type: float_type, call: cos },
    Function { name: "tan", min_args: 1, max_args: 1, result_type: float_type, call: tan },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_trigonometry_and_null_propagation() {
        assert_eq!(sin(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert_eq!(cos(&[Value::Integer(0)]).unwrap(), Value::Float(1.0));
        assert_eq!(tan(&[Value::Null]).unwrap(), Value::Null);
    }
}
