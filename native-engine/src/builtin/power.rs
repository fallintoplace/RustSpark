use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn power(args: &[Value]) -> Result<Value> {
    let Some(base) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(exponent) = super::number(&args[1])? else { return Ok(Value::Null) };
    super::finite(base.powf(exponent))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "power", min_args: 2, max_args: 2, result_type: float_type, call: power },
    Function { name: "pow", min_args: 2, max_args: 2, result_type: float_type, call: power },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pow_is_typed_and_rejects_unbounded_results() {
        assert_eq!(power(&[Value::Integer(2), Value::Integer(10)]).unwrap(), Value::Float(1024.0));
        assert!(power(&[Value::Integer(0), Value::Integer(-1)]).is_err());
        assert_eq!(power(&[Value::Null, Value::Integer(9)]).unwrap(), Value::Null);
    }
}
