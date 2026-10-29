use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|&kind| kind == DataType::Integer) { Ok(DataType::Integer) }
    else { Err(Error::Type("shift SQL functions require integer operands".into())) }
}
fn parts(args: &[Value]) -> Result<Option<(i64, u32)>> {
    let Some(value) = super::integer(&args[0])? else { return Ok(None) };
    let Some(shift) = super::integer(&args[1])? else { return Ok(None) };
    // Spark follows JVM long-shift semantics: only the low six bits count.
    let shift = (shift as u32) & 63;
    Ok(Some((value, shift)))
}
fn left(args: &[Value]) -> Result<Value> {
    Ok(parts(args)?.map_or(Value::Null, |(n, shift)| Value::Integer(n.wrapping_shl(shift))))
}
fn right(args: &[Value]) -> Result<Value> {
    Ok(parts(args)?.map_or(Value::Null, |(n, shift)| Value::Integer(n >> shift)))
}
fn unsigned_right(args: &[Value]) -> Result<Value> {
    Ok(parts(args)?.map_or(Value::Null, |(n, shift)| Value::Integer(((n as u64) >> shift) as i64)))
}
pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "shiftleft", min_args: 2, max_args: 2, result_type: output, call: left },
    Function { name: "shiftright", min_args: 2, max_args: 2, result_type: output, call: right },
    Function { name: "shiftrightunsigned", min_args: 2, max_args: 2, result_type: output, call: unsigned_right },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arithmetic_and_logical_right_shifts_differ() {
        assert_eq!(left(&[Value::Integer(1), Value::Integer(3)]).unwrap(), Value::Integer(8));
        assert_eq!(right(&[Value::Integer(-2), Value::Integer(1)]).unwrap(), Value::Integer(-1));
        assert_eq!(unsigned_right(&[Value::Integer(-2), Value::Integer(1)]).unwrap(), Value::Integer(i64::MAX));
        assert_eq!(left(&[Value::Integer(1), Value::Integer(64)]).unwrap(), Value::Integer(1));
        assert_eq!(left(&[Value::Integer(1), Value::Integer(65)]).unwrap(), Value::Integer(2));
        assert_eq!(left(&[Value::Integer(1), Value::Integer(-1)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(right(&[Value::Integer(-4), Value::Integer(65)]).unwrap(), Value::Integer(-2));
        assert_eq!(unsigned_right(&[Value::Integer(-2), Value::Integer(65)]).unwrap(), Value::Integer(i64::MAX));
        assert_eq!(left(&[Value::Null, Value::Integer(64)]).unwrap(), Value::Null);
    }
}
