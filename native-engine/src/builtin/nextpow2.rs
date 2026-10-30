//! Checked native next power of two for nonnegative BIGINT inputs.
use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("NEXTPOW2 requires BIGINT input".into()))
    }
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    if value < 0 {
        return Err(Error::Execution("NEXTPOW2 requires a nonnegative integer".into()));
    }
    let next = (value as u64).checked_next_power_of_two()
        .and_then(|result| i64::try_from(result).ok())
        .ok_or_else(|| Error::Execution("NEXTPOW2 result exceeds BIGINT range".into()))?;
    Ok(Value::Integer(next))
}

pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "nextpow2", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rounds_up_and_rejects_overflow() {
        assert_eq!(evaluate(&[Value::Integer(0)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Integer(1)]).unwrap(), Value::Integer(1));
        assert_eq!(evaluate(&[Value::Integer(9)]).unwrap(), Value::Integer(16));
        assert_eq!(evaluate(&[Value::Integer(1_i64 << 62)]).unwrap(), Value::Integer(1_i64 << 62));
        assert!(evaluate(&[Value::Integer((1_i64 << 62) + 1)]).is_err());
        assert!(evaluate(&[Value::Integer(-1)]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Float]).is_err());
        assert!(crate::builtin::supports("NEXTPOW2"));
    }
}
