use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    float_type(types)?;
    Ok(DataType::Integer)
}
fn checked_integer(value: f64) -> Result<Value> {
    if !value.is_finite() || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value) {
        return Err(Error::Execution("rounded SQL value is outside signed 64-bit range".into()));
    }
    Ok(Value::Integer(value as i64))
}
fn floor(args: &[Value]) -> Result<Value> {
    match super::number(&args[0])? {
        Some(v) => checked_integer(v.floor()),
        None => Ok(Value::Null),
    }
}
fn ceil(args: &[Value]) -> Result<Value> {
    match super::number(&args[0])? {
        Some(v) => checked_integer(v.ceil()),
        None => Ok(Value::Null),
    }
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "floor", min_args: 1, max_args: 1, result_type: output, call: floor },
    Function { name: "ceil", min_args: 1, max_args: 1, result_type: output, call: ceil },
    Function { name: "ceiling", min_args: 1, max_args: 1, result_type: output, call: ceil },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn floor_and_ceil_are_signed_and_checked() {
        assert_eq!(floor(&[Value::Float(-1.2)]).unwrap(), Value::Integer(-2));
        assert_eq!(ceil(&[Value::Float(-1.2)]).unwrap(), Value::Integer(-1));
        assert!(ceil(&[Value::Float(f64::MAX)]).is_err());
        assert_eq!(floor(&[Value::Null]).unwrap(), Value::Null);
    }
}
