use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let mut crc = !0_u32;
    for byte in text.bytes() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
        }
    }
    Ok(Value::Integer(i64::from(!crc)))
}
pub(super) const FUNCTIONS: &[Function] = &[Function {
    name: "crc32", min_args: 1, max_args: 1, result_type: output, call: evaluate,
}];

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_ieee_crc32_reference_vectors() {
        assert_eq!(evaluate(&[Value::Text("123456789".into())]).unwrap(), Value::Integer(0xCBF4_3926));
        assert_eq!(evaluate(&[Value::Text("".into())]).unwrap(), Value::Integer(0));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
