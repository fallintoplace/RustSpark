//! Native two-argument arctangent in degrees.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(y) = super::number(&args[0])? else { return Ok(Value::Null) };
    let Some(x) = super::number(&args[1])? else { return Ok(Value::Null) };
    super::finite(y.atan2(x).to_degrees())
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "atan2d", min_args: 2, max_args: 2,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn degrees_respect_quadrants_and_nulls() {
        assert_eq!(evaluate(&[Value::Integer(1), Value::Integer(0)]).unwrap(), Value::Float(90.0));
        assert_eq!(evaluate(&[Value::Integer(-1), Value::Integer(0)]).unwrap(), Value::Float(-90.0));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
        assert!(float_type(&[DataType::Text, DataType::Integer]).is_err());
        assert!(crate::builtin::supports("ATAN2D"));
    }
}
