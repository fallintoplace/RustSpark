//! Three-dimensional hypotenuse without intermediate squaring overflow.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(y) = super::number(&args[1])? else { return Ok(Value::Null) };
    let Some(z) = super::number(&args[2])? else { return Ok(Value::Null) };
    super::finite(x.hypot(y).hypot(z))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "hypot3", min_args: 3, max_args: 3,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn computes_three_dimensional_distance_and_handles_nulls() {
        assert_eq!(evaluate(&[Value::Integer(2), Value::Integer(3), Value::Integer(6)]).unwrap(), Value::Float(7.0));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(3), Value::Integer(6)]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Float(f64::MAX), Value::Float(f64::MAX), Value::Float(1.0)]).is_err());
        assert!(float_type(&[DataType::Integer, DataType::Text, DataType::Integer]).is_err());
        assert!(crate::builtin::supports("HYPOT3"));
    }
}
