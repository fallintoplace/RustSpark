//! Native Heaviside step function with a half-value at zero.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    if x.is_nan() {
        return super::finite(x);
    }
    let result = if x < 0.0 { 0.0 } else if x > 0.0 { 1.0 } else { 0.5 };
    Ok(Value::Float(result))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "heaviside", min_args: 1, max_args: 1,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn step_function_handles_zero_signs_and_null() {
        assert_eq!(evaluate(&[Value::Integer(-3)]).unwrap(), Value::Float(0.0));
        assert_eq!(evaluate(&[Value::Integer(3)]).unwrap(), Value::Float(1.0));
        assert_eq!(evaluate(&[Value::Float(-0.0)]).unwrap(), Value::Float(0.5));
        assert_eq!(evaluate(&[Value::Float(0.0)]).unwrap(), Value::Float(0.5));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Float(f64::NAN)]).is_err());
        assert!(float_type(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("HEAVISIDE"));
    }
}
