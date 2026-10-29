use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == types[1] || matches!((types[0], types[1]),
        (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer)) {
        Ok(types[0])
    } else {
        Err(Error::Type("NULLIF requires comparable arguments".into()))
    }
}
use std::cmp::Ordering;

fn evaluate(args: &[Value]) -> Result<Value> {
    if args[0].is_null() { return Ok(Value::Null); }
    if args[1].is_null() { return Ok(args[0].clone()); }
    if args[0].sql_compare(&args[1])? == Some(Ordering::Equal) {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "nullif", min_args: 2, max_args: 2, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn equal_nonnull_values_become_null() {
        assert_eq!(evaluate(&[Value::Integer(5), Value::Integer(5)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(5), Value::Integer(6)]).unwrap(), Value::Integer(5));
        assert_eq!(evaluate(&[Value::Integer(5), Value::Null]).unwrap(), Value::Integer(5));
        assert_eq!(output(&[DataType::Integer, DataType::Float]).unwrap(), DataType::Integer);
        assert_eq!(output(&[DataType::Float, DataType::Integer]).unwrap(), DataType::Float);
        assert!(output(&[DataType::Text, DataType::Integer]).is_err());
        assert_eq!(evaluate(&[Value::Integer(5), Value::Float(5.0)]).unwrap(), Value::Null);
        assert_eq!(evaluate(&[Value::Integer(9_007_199_254_740_993), Value::Float(9_007_199_254_740_992.0)]).unwrap(), Value::Integer(9_007_199_254_740_993));
    }
}
