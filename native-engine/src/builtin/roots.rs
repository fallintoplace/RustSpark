use super::{float_type, Function};
use crate::{scalar::Value, schema::DataType, Result};

fn unary(args: &[Value], f: fn(f64) -> f64) -> Result<Value> {
    let Some(value) = super::number(&args[0])? else { return Ok(Value::Null) };
    super::finite(f(value))
}
fn sqrt(args: &[Value]) -> Result<Value> { unary(args, f64::sqrt) }
fn cbrt(args: &[Value]) -> Result<Value> { unary(args, f64::cbrt) }


fn isinfinite_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Integer | DataType::Float) {
        Ok(DataType::Boolean)
    } else {
        Err(crate::Error::Type("ISINFINITE requires numeric input".into()))
    }
}
fn isinfinite(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Float(value) => Ok(Value::Boolean(value.is_infinite())),
        Value::Integer(_) => Ok(Value::Boolean(false)),
        Value::Null => Ok(Value::Null),
        _ => Err(crate::Error::Type("ISINFINITE requires numeric input".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "isinfinite", min_args: 1, max_args: 1, result_type: isinfinite_type, call: isinfinite },
    Function { name: "sqrt", min_args: 1, max_args: 1, result_type: float_type, call: sqrt },
    Function { name: "cbrt", min_args: 1, max_args: 1, result_type: float_type, call: cbrt },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roots_handle_negative_and_null_values() {
        assert_eq!(sqrt(&[Value::Integer(9)]).unwrap(), Value::Float(3.0));
        assert_eq!(cbrt(&[Value::Integer(-8)]).unwrap(), Value::Float(-2.0));
        assert!(sqrt(&[Value::Integer(-1)]).is_err());
        assert_eq!(sqrt(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod isinfinite_extension_tests {
    use super::*;
    #[test]
    fn handles_special_values_and_nulls() {
        assert!(crate::builtin::supports("isinfinite"));
        assert_eq!(isinfinite(&[Value::Float(f64::INFINITY)]).unwrap(), Value::Boolean(true));
        assert_eq!(isinfinite(&[Value::Float(f64::NEG_INFINITY)]).unwrap(), Value::Boolean(true));
        assert_eq!(isinfinite(&[Value::Float(f64::NAN)]).unwrap(), Value::Boolean(false));
        assert_eq!(isinfinite(&[Value::Integer(99)]).unwrap(), Value::Boolean(false));
        assert_eq!(isinfinite(&[Value::Null]).unwrap(), Value::Null);
    }
}
