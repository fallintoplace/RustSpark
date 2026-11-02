use super::{float_type, Function};
use crate::{scalar::Value, Result};
fn sin(args: &[Value]) -> Result<Value> { super::unary_float(args, f64::sin) }
fn cos(args: &[Value]) -> Result<Value> { super::unary_float(args, f64::cos) }
fn tan(args: &[Value]) -> Result<Value> { super::unary_float(args, f64::tan) }
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
