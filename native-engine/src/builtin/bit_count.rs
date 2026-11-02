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

fn getbit_type(types: &[DataType]) -> Result<DataType> {
    if types.iter().all(|kind| *kind == DataType::Integer) {
        Ok(DataType::Integer)
    } else {
        Err(Error::Type("GETBIT expects integer value and position".into()))
    }
}

fn getbit(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null); };
    let Some(position) = super::integer(&args[1])? else { return Ok(Value::Null); };
    let bit = u32::try_from(position)
        .ok().filter(|bit| *bit < 64)
        .ok_or_else(|| Error::Execution("GETBIT position must be within [0, 63]".into()))?;
    Ok(Value::Integer(((value as u64) >> bit & 1) as i64))
}

fn bitmap_bit_position(args: &[Value]) -> Result<Value> {
    let Some(value) = super::integer(&args[0])? else { return Ok(Value::Null); };
    // The bucket size is a power of two. Wrapping arithmetic preserves
    // Euclidean modulo for negative values, including i64::MIN.
    let offset = value.wrapping_sub(1) & 32_767;
    Ok(Value::Integer(offset))
}

fn bitmap_bit_position_type(types: &[DataType]) -> Result<DataType> {
    output(types)
}

pub(super) const FUNCTIONS: &[Function] = &[
    Function { name: "getbit", min_args: 2, max_args: 2, result_type: getbit_type, call: getbit },
    Function { name: "bit_reverse", min_args: 1, max_args: 1, result_type: bit_reverse_type, call: bit_reverse },
    Function { name: "bit_count", min_args: 1, max_args: 1, result_type: output, call: bit_count },
    Function { name: "bitmap_bit_position", min_args: 1, max_args: 1,
        result_type: bitmap_bit_position_type, call: bitmap_bit_position },
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

#[cfg(test)]
mod spark_getbit_sql_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};
    #[test]
    fn bit_positions_match_spark() {
        assert!(crate::builtin::supports("getbit"));
        assert_eq!(getbit(&[Value::Integer(11), Value::Integer(0)]).unwrap(), Value::Integer(1));
        assert_eq!(getbit(&[Value::Integer(11), Value::Integer(2)]).unwrap(), Value::Integer(0));
        assert_eq!(getbit(&[Value::Integer(i64::MIN), Value::Integer(63)]).unwrap(), Value::Integer(1));
        assert_eq!(getbit(&[Value::Null, Value::Integer(0)]).unwrap(), Value::Null);
        assert!(getbit(&[Value::Integer(11), Value::Integer(-1)]).is_err());
        assert!(getbit(&[Value::Integer(11), Value::Integer(64)]).is_err());
        let plan = sql::parse_sql("SELECT GETBIT(11, 0), GETBIT(11, 2)").unwrap();
        assert_eq!(executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1), Value::Integer(0)]]);
    }
}

#[cfg(test)]
mod spark_bitmap_position_tests {
    use super::*;
    use crate::{catalog::Catalog, executor, sql};

    #[test]
    fn computes_offsets_without_overflow_on_bigint_extremes() {
        for (input, expected) in [
            (1, 0), (123, 122), (32_768, 32_767),
            (32_769, 0), (0, 32_767), (-1, 32_766),
            (i64::MAX, ((i128::from(i64::MAX) - 1) % 32_768) as i64),
        ] {
            assert_eq!(bitmap_bit_position(&[Value::Integer(input)]).unwrap(),
                Value::Integer(expected));
        }
        assert_eq!(bitmap_bit_position(&[Value::Integer(i64::MIN)]).unwrap(),
            Value::Integer((i128::from(i64::MIN) - 1).rem_euclid(32_768) as i64));
        assert_eq!(bitmap_bit_position(&[Value::Null]).unwrap(), Value::Null);
        assert!(bitmap_bit_position_type(&[DataType::Text]).is_err());
    }

    #[test]
    fn native_sql_uses_spark_bucket_offsets() {
        let plan = sql::parse_sql("SELECT BITMAP_BIT_POSITION(1), BITMAP_BIT_POSITION(123)").unwrap();
        assert_eq!(executor::execute(&plan, &Catalog::new()).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(0), Value::Integer(122)]]);
    }
}

#[cfg(test)]
mod bitmap_masked_position_tests {
    use super::*;

    #[test]
    fn masked_offsets_match_wide_euclidean_modulo() {
        for value in -65_536_i64..=65_536 {
            let expected = (i128::from(value) - 1).rem_euclid(32_768) as i64;
            assert_eq!(bitmap_bit_position(&[Value::Integer(value)]).unwrap(),
                Value::Integer(expected));
        }
        for value in [i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX] {
            let expected = (i128::from(value) - 1).rem_euclid(32_768) as i64;
            assert_eq!(bitmap_bit_position(&[Value::Integer(value)]).unwrap(),
                Value::Integer(expected));
        }
    }
}
