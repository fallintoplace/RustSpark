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
pub(super) const FUNCTIONS: &[Function] = &[
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
