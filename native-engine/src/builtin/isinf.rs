//! Native IEEE-754 infinity predicate for numeric SQL values.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) {
        Ok(DataType::Boolean)
    } else {
        Err(Error::Type("ISINF requires numeric input".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Integer(_) => Ok(Value::Boolean(false)),
        Value::Float(number) => Ok(Value::Boolean(number.is_infinite())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::Type("ISINF requires numeric input".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "isinf", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_infinity_nan_finite_values_and_nulls() {
        assert_eq!(evaluate(&[Value::Float(f64::INFINITY)]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Float(f64::NEG_INFINITY)]).unwrap(), Value::Boolean(true));
        assert_eq!(evaluate(&[Value::Float(f64::NAN)]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Float(0.0)]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Integer(i64::MAX)]).unwrap(), Value::Boolean(false));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("ISINF"));
    }
}
