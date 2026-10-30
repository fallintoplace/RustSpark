//! Native softsign activation function with bounded output.
use super::{float_type, Function};
use crate::{scalar::Value, Result};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(x) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(x / (1.0 + x.abs()))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "softsign", min_args: 1, max_args: 1,
    result_type: float_type, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    #[test]
    fn activation_is_bounded_and_odd_with_null_propagation() {
        assert_eq!(evaluate(&[Value::Integer(3)]).unwrap(), Value::Float(0.75));
        assert_eq!(evaluate(&[Value::Integer(-3)]).unwrap(), Value::Float(-0.75));
        assert_eq!(evaluate(&[Value::Integer(0)]).unwrap(), Value::Float(0.0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Float(f64::NAN)]).is_err());
        assert!(float_type(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("SOFTSIGN"));
    }
}
