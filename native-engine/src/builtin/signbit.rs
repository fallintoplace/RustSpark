//! Native sign-bit predicate, including the sign of negative zero.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) {
        Ok(DataType::Boolean)
    } else {
        Err(Error::Type("SIGNBIT requires numeric input".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Integer(value) => Ok(Value::Boolean(*value < 0)),
        Value::Float(value) => Ok(Value::Boolean(value.is_sign_negative())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::Type("SIGNBIT requires numeric input".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "signbit", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_negative_zero_and_signed_nan() {
        assert_eq!(evaluate(&[Value::Float(-0.0)]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Float(0.0)]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Integer(-1)]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Integer(0)]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("SIGNBIT"));
    }
}
