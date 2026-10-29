use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|&type_| type_ == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("bitwise SQL functions require integer arguments".into())) }
}
fn binary(args: &[Value], f: fn(i64, i64) -> i64) -> Result<Value> {
    let Some(a) = super::integer(&args[0])? else { return Ok(Value::Null) };
    let Some(b) = super::integer(&args[1])? else { return Ok(Value::Null) };
    Ok(Value::Integer(f(a, b)))
}
fn and(args: &[Value]) -> Result<Value> { binary(args, |a, b| a & b) }
fn or(args: &[Value]) -> Result<Value> { binary(args, |a, b| a | b) }
fn xor(args: &[Value]) -> Result<Value> { binary(args, |a, b| a ^ b) }
fn not(args: &[Value]) -> Result<Value> {
    Ok(super::integer(&args[0])?.map_or(Value::Null, |value| Value::Integer(!value)))
}

fn rotate_right(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(count) = super::integer(&args[1])? else { return Ok(Value::Null); };
    Ok(Value::Integer(number.rotate_right(count.rem_euclid(64) as u32)))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "rotate_right", min_args: 2, max_args: 2, result_type: output, call: rotate_right },
    Function { name: "bitwise_and", min_args: 2, max_args: 2, result_type: output, call: and },
    Function { name: "bitwise_or", min_args: 2, max_args: 2, result_type: output, call: or },
    Function { name: "bitwise_xor", min_args: 2, max_args: 2, result_type: output, call: xor },
    Function { name: "bitwise_not", min_args: 1, max_args: 1, result_type: output, call: not },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bitwise_operations_preserve_signed_words() {
        assert_eq!(and(&[Value::Integer(12), Value::Integer(10)]).unwrap(), Value::Integer(8));
        assert_eq!(or(&[Value::Integer(12), Value::Integer(10)]).unwrap(), Value::Integer(14));
        assert_eq!(xor(&[Value::Integer(12), Value::Integer(10)]).unwrap(), Value::Integer(6));
        assert_eq!(not(&[Value::Integer(0)]).unwrap(), Value::Integer(-1));
    }
}

#[cfg(test)]
mod rotate_right_extension_tests {
    use super::*;
    #[test]
    fn checks_integer_boundaries_and_nulls() {
        assert!(crate::builtin::supports("rotate_right"));
        assert_eq!(rotate_right(&[Value::Integer(2), Value::Integer(1)]).unwrap(), Value::Integer(1));
        assert_eq!(rotate_right(&[Value::Integer(1), Value::Integer(1)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(rotate_right(&[Value::Integer(2), Value::Integer(65)]).unwrap(), Value::Integer(1));
        assert_eq!(rotate_right(&[Value::Integer(-1), Value::Integer(7)]).unwrap(), Value::Integer(-1));
        assert_eq!(rotate_right(&[Value::Null, Value::Integer(2)]).unwrap(), Value::Null);
    }
}
