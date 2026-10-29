use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let length = i64::try_from(text.len())
        .map_err(|_| Error::Execution("OCTET_LENGTH overflow".into()))?;
    Ok(Value::Integer(length))
}


fn is_ascii_type(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Boolean)
}
fn is_ascii(args: &[Value]) -> Result<Value> {
    let Some(value) = super::string(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Boolean(value.is_ascii()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "is_ascii", min_args: 1, max_args: 1, result_type: is_ascii_type, call: is_ascii },Function {
    name: "octet_length", min_args: 1, max_args: 1,
    result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supports_unicode_and_null() {
        assert_eq!(evaluate(&[Value::Text("é🙂".into())]).unwrap(), Value::Integer(6));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Integer]).is_err());
    }
}

#[cfg(test)]
mod is_ascii_extension_tests {
    use super::*;
    #[test]
    fn checks_data_shapes_and_nulls() {
        assert!(crate::builtin::supports("is_ascii"));
        assert_eq!(is_ascii(&[Value::Text("ABC_012".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(is_ascii(&[Value::Text("café".into())]).unwrap(), Value::Boolean(false));
        assert_eq!(is_ascii(&[Value::Text("".into())]).unwrap(), Value::Boolean(true));
        assert_eq!(is_ascii(&[Value::Null]).unwrap(), Value::Null);
    }
}
