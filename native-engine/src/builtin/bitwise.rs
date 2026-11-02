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

fn bitmap_bucket_number(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let bucket = (i128::from(value) - 1).div_euclid(32_768) + 1;
    Ok(Value::Integer(bucket as i64))
}

fn bitmap_bucket_number_type(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("BITMAP_BUCKET_NUMBER expects integer input".into())) }
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "rotate_right", min_args: 2, max_args: 2, result_type: output, call: rotate_right },
    Function { name: "bitmap_bucket_number", min_args: 1, max_args: 1,
        result_type: bitmap_bucket_number_type, call: bitmap_bucket_number },
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

#[cfg(test)]
mod spark_bitmap_bucket_number_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn handles_positive_and_negative_bucket_boundaries() {
        for (input, expected) in [
            (0, 0), (1, 1), (123, 1),
            (32_768, 1), (32_769, 2), (-1, 0),
            (-32_768, -1), (i64::MAX, (i128::from(i64::MAX) - 1).div_euclid(32_768) as i64 + 1),
        ] {
            assert_eq!(bitmap_bucket_number(&[Value::Integer(input)]).unwrap(),
                Value::Integer(expected));
        }
        assert_eq!(bitmap_bucket_number(&[Value::Integer(i64::MIN)]).unwrap(),
            Value::Integer(((i128::from(i64::MIN) - 1).div_euclid(32_768) + 1) as i64));
        assert_eq!(bitmap_bucket_number(&[Value::Null]).unwrap(), Value::Null);
        assert!(bitmap_bucket_number_type(&[DataType::Float]).is_err());
    }

    #[test]
    fn sql_exposes_bucket_number() {
        let plan = sql::parse_sql("SELECT BITMAP_BUCKET_NUMBER(123), BITMAP_BUCKET_NUMBER(0)").unwrap();
        assert_eq!(executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1), Value::Integer(0)]]);
    }
}
