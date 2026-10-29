use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) { Ok(DataType::Text) }
    else { Err(Error::Type("BIN requires a numeric argument".into())) }
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let value = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Integer(value) => *value,
        Value::Float(value) => match Value::Float(*value).cast_to(DataType::Integer)? {
            Value::Integer(number) => number,
            _ => unreachable!("float-to-integer cast must return integer"),
        },
        _ => return Err(Error::Type("BIN requires a number".into())),
    };
    Ok(Value::Text(format!("{:b}", value as u64)))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "bin", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_twos_complement_and_truncates_floats() {
        assert_eq!(evaluate(&[Value::Integer(13)]).unwrap(), Value::Text("1101".into()));
        assert_eq!(evaluate(&[Value::Float(13.3)]).unwrap(), Value::Text("1101".into()));
        assert_eq!(evaluate(&[Value::Integer(-1)]).unwrap(), Value::Text("1".repeat(64)));
        assert!(evaluate(&[Value::Float(f64::INFINITY)]).is_err());
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
