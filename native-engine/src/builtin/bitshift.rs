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

fn rotate_left(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(count) = super::integer(&args[1])? else { return Ok(Value::Null); };
    Ok(Value::Integer(number.rotate_left((count as u32) & 63)))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "rotate_left", min_args: 2, max_args: 2, result_type: output, call: rotate_left },
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

#[cfg(test)]
mod rotate_left_extension_tests {
    use super::*;
    #[test]
    fn checks_integer_boundaries_and_nulls() {
        assert!(crate::builtin::supports("rotate_left"));
        assert_eq!(rotate_left(&[Value::Integer(1), Value::Integer(1)]).unwrap(), Value::Integer(2));
        assert_eq!(rotate_left(&[Value::Integer(1), Value::Integer(65)]).unwrap(), Value::Integer(2));
        assert_eq!(rotate_left(&[Value::Integer(1), Value::Integer(-1)]).unwrap(), Value::Integer(i64::MIN));
        assert_eq!(rotate_left(&[Value::Integer(-1), Value::Integer(7)]).unwrap(), Value::Integer(-1));
        assert_eq!(rotate_left(&[Value::Null, Value::Integer(1)]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod rotate_mask_regression_tests {
    use super::*;
    #[test]
    fn masked_rotation_matches_euclidean_remainder() {
        for shift in [i64::MIN, -129, -64, -1, 0, 1, 64, 65, i64::MAX] {
            let value = 0x1234_5678_9abc_def0_i64;
            let actual = rotate_left(&[Value::Integer(value), Value::Integer(shift)]).unwrap();
            assert_eq!(actual, Value::Integer(value.rotate_left(shift.rem_euclid(64) as u32)));
        }
    }
}
