use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Result};

fn unary(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn sqrt(args: &[Value]) -> Result<Value> { unary(args, f64::sqrt) }
fn cbrt(args: &[Value]) -> Result<Value> { unary(args, f64::cbrt) }

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "sqrt", min_args: 1, max_args: 1, result_type: float_type, call: sqrt },
    Function { name: "cbrt", min_args: 1, max_args: 1, result_type: float_type, call: cbrt },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roots_handle_negative_and_null_values() {
        assert_eq!(sqrt(&[Value::Integer(9)]).unwrap(), Value::Float(3.0));
        assert_eq!(cbrt(&[Value::Integer(-8)]).unwrap(), Value::Float(-2.0));
        assert!(sqrt(&[Value::Integer(-1)]).is_err());
        assert_eq!(sqrt(&[Value::Null]).unwrap(), Value::Null);
    }
}
