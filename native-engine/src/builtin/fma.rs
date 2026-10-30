//! Native fused multiply-add using a single floating point rounding.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(a) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(b) = super::number(&args[1])? else { return Ok(Value::Null) };
    let Some(c) = super::number(&args[2])? else { return Ok(Value::Null) };
    super::finite(a.mul_add(b, c))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "fma", min_args: 3, max_args: 3,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn fused_math_handles_nulls_and_nonfinite_results() {
        assert_eq!(evaluate(&[Value::Integer(2), Value::Integer(3), Value::Integer(4)]).unwrap(), Value::Float(10.0));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(3), Value::Integer(4)]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Float(f64::MAX), Value::Float(f64::MAX), Value::Float(1.0)]).is_err());
        assert!(float_type(&[DataType::Text, DataType::Float, DataType::Float]).is_err());
        assert!(crate::builtin::supports("FMA"));
    }
}
