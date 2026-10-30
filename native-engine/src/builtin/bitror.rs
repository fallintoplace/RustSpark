//! Native 64-bit rotation to the right, retaining every input bit.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("BITROR requires two BIGINT inputs".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let Some(shift) = super::integer(&args[1])? else { return Ok(Value::Null) };
    if shift < 0 {
        return Err(Error::Execution("BITROR shift count must be nonnegative".into()));
    }
    Ok(Value::Integer(value.rotate_right((shift % 64) as u32)))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "bitror", min_args: 2, max_args: 2,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotates_bits_and_checks_shift_bounds() {
        assert_eq!(evaluate(&[Value::Integer(2), Value::Integer(1)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Integer(1), Value::Integer(1)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(evaluate(&[Value::Integer(2), Value::Integer(65)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
        assert!(evaluate(&[Value::Integer(1), Value::Integer(-1)]).is_err());
        assert!(output(&[DataType::Integer, DataType::Float]).is_err());
        assert!(crate::builtin::supports("BITROR"));
    }
}
