use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn hypot(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(y) = super::number(&args[1])? else { return Ok(Value::Null) };
    super::finite(x.hypot(y))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "hypot", min_args: 2, max_args: 2, result_type: float_type, call: hypot },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hypot_avoids_intermediate_square_overflow() {
        assert_eq!(hypot(&[Value::Integer(3), Value::Integer(4)]).unwrap(), Value::Float(5.0));
        assert_eq!(hypot(&[Value::Null, Value::Integer(4)]).unwrap(), Value::Null);
        assert!(hypot(&[Value::Float(f64::MAX), Value::Float(f64::MAX)]).is_err());
    }
}
