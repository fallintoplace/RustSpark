use super::Function;
use crate::{scalar::Value, schema::DataType, Error, Result};

const BITS: i64 = 32_768;

fn output(types: &[DataType]) -> Result<DataType> {
    if types[0] == DataType::Integer { Ok(DataType::Integer) }
    else { Err(Error::Type("bitmap index functions require BIGINT".into())) }
}

fn bit_position(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let position = if number > 0 {
        (number - 1) % BITS
    } else {
        (number.unsigned_abs() % BITS as u64) as i64
    };
    Ok(Value::Integer(position))
}

fn bucket_number(args: &[Value]) -> Result<Value> {
    let Some(number) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let bucket = if number > 0 {
        ((number - 1) / BITS) + 1
    } else {
        number / BITS
    };
    Ok(Value::Integer(bucket))
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "bitmap_bit_position", min_args: 1, max_args: 1,
        result_type: output, call: bit_position },
    Function { name: "bitmap_bucket_number", min_args: 1, max_args: 1,
        result_type: output, call: bucket_number },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spark_bucket_and_bit_position_cover_positive_negative_and_boundaries() {
        for (number, bucket, position) in [
            (0, 0, 0), (1, 1, 0), (32_768, 1, 32_767),
            (32_769, 2, 0), (-32_767, 0, 32_767), (-32_768, -1, 0),
            (i64::MIN, i64::MIN / 32_768, 0),
        ] {
            let value = [Value::Integer(number)];
            assert_eq!(bucket_number(&value).unwrap(), Value::Integer(bucket));
            assert_eq!(bit_position(&value).unwrap(), Value::Integer(position));
        }
        assert_eq!(bucket_number(&[Value::Null]).unwrap(), Value::Null);
        assert!(output(&[DataType::Float]).is_err());
    }
}
