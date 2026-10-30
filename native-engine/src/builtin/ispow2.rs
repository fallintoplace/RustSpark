//! Native power-of-two predicate for positive BIGINT values.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer {
        Ok(DataType::Boolean)
    } else {
        Err(Error::Type("ISPOW2 requires BIGINT input".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Boolean(value > 0 && value.is_power_of_two()))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "ispow2", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checks_positive_power_of_two_boundaries() {
        for value in [1, 2, 4, 1024, 1_i64 << 62] {
            assert_eq!(evaluate(&[Value::Integer(value)]).unwrap(), Value::Boolean(true));
        }
        for value in [0, -2, 3, i64::MAX] {
            assert_eq!(evaluate(&[Value::Integer(value)]).unwrap(), Value::Boolean(false));
        }
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Text]).is_err());
        assert!(crate::builtin::supports("ISPOW2"));
    }
}
