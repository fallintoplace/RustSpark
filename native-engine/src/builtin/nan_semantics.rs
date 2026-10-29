use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn isnan_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Float | DataType::Integer) { Ok(DataType::Boolean) }
    else { Err(Error::Type("ISNAN requires a numeric input".into())) }
}
fn nanvl_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Float) { Ok(DataType::Float) }
    else { Err(Error::Type("NANVL requires floating-point inputs".into())) }
}
fn isnan(args: &[Value]) -> Result<Value> {
    Ok(Value::Boolean(match &args[0] {
        Value::Float(n) => n.is_nan(),
        Value::Integer(_) | Value::Null => false,
        _ => return Err(Error::Type("ISNAN expects numeric value".into())),
    }))
}
fn nanvl(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Float(n) if n.is_nan() => Ok(args[1].clone()),
        Value::Float(_) | Value::Null => Ok(args[0].clone()),
        _ => Err(Error::Type("NANVL expects a floating-point value".into())),
    }
}

fn isfinite_type(types: &[DataType]) -> Result<DataType> {
    if matches!(types[0], DataType::Float | DataType::Integer | DataType::Text) {
        Ok(DataType::Boolean)
    } else {
        Err(Error::Type("ISFINITE requires numeric input".into()))
    }
}

fn isfinite(args: &[Value]) -> Result<Value> {
    match &args[0] {
        Value::Integer(_) => Ok(Value::Boolean(true)),
        Value::Float(number) => Ok(Value::Boolean(number.is_finite())),
        Value::Null => Ok(Value::Null),
        _ => Err(Error::Type("ISFINITE requires numeric input".into())),
    }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "isfinite", min_args: 1, max_args: 1, result_type: isfinite_type, call: isfinite },
    Function { name: "isnan", min_args: 1, max_args: 1, result_type: isnan_type, call: isnan },
    Function { name: "nanvl", min_args: 2, max_args: 2, result_type: nanvl_type, call: nanvl },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_nan_is_not_the_same_as_null() {
        assert_eq!(isnan(&[Value::Null]).unwrap(), Value::Boolean(false));
        assert_eq!(isnan(&[Value::Float(f64::NAN)]).unwrap(), Value::Boolean(true));
        assert_eq!(nanvl(&[Value::Float(f64::NAN), Value::Float(42.0)]).unwrap(), Value::Float(42.0));
        assert_eq!(nanvl(&[Value::Null, Value::Float(5.0)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod isfinite_extension_tests {
    use super::*;
    #[test]
    fn checks_finite_numbers_without_rounding_large_integers() {
        assert!(crate::builtin::supports("isfinite"));
        assert_eq!(isfinite(&[Value::Integer(i64::MAX)]).unwrap(), Value::Boolean(true));
        assert_eq!(isfinite(&[Value::Float(f64::NAN)]).unwrap(), Value::Boolean(false));
        assert_eq!(isfinite(&[Value::Float(f64::INFINITY)]).unwrap(), Value::Boolean(false));
        assert_eq!(isfinite(&[Value::Float(0.0)]).unwrap(), Value::Boolean(true));
        assert_eq!(isfinite(&[Value::Null]).unwrap(), Value::Null);
        assert!(isfinite(&[Value::Text("123".into())]).is_err());
    }
}
