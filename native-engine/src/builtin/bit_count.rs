use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("BIT_COUNT requires an integer".into())) }
}
fn bit_count(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null) };
    Ok(Value::Integer(value.count_ones() as i64))
}

fn bit_reverse_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("BIT_REVERSE requires integer input".into())) }
}
fn bit_reverse(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null); };
    Ok(Value::Integer(value.reverse_bits()))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bit_reverse", min_args: 1, max_args: 1, result_type: bit_reverse_type, call: bit_reverse },
    Function { name: "bit_count", min_args: 1, max_args: 1, result_type: output, call: bit_count },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counts_twos_complement_bits() {
        assert_eq!(bit_count(&[Value::Integer(15)]).unwrap(), Value::Integer(4));
        assert_eq!(bit_count(&[Value::Integer(-1)]).unwrap(), Value::Integer(64));
        assert_eq!(bit_count(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bit_reverse_extension_tests {
    use super::*;
    #[test]
    fn handles_special_values_and_nulls() {
        assert!(crate::builtin::supports("bit_reverse"));
        assert_eq!(bit_reverse(&[Value::Integer(1)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(bit_reverse(&[Value::Integer(0)]).unwrap(), Value::Integer(0));
        assert_eq!(bit_reverse(&[Value::Integer(-1)]).unwrap(), Value::Integer(-1));
        assert_eq!(bit_reverse(&[Value::Null]).unwrap(), Value::Null);
        let value = Value::Integer(0x1234_5678_9abc_def0);
        let reversed = bit_reverse(&[value.clone()]).unwrap();
        assert_eq!(bit_reverse(&[reversed]).unwrap(), value);
    }
}
