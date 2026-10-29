use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
const TABLE: [u32; 256] = {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let mut crc = !0_u32;
    for byte in text.bytes() {
        let index = ((crc as u8) ^ byte) as usize;
        crc = (crc >> 8) ^ TABLE[index];
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
        assert_eq!(evaluate(&[Value::Text("The quick brown fox jumps over the lazy dog".into())]).unwrap(),
            Value::Integer(0x414F_A339));
        assert_eq!(evaluate(&[Value::Text("a".repeat(100_000))]).unwrap(),
            Value::Integer(0xA2BC_F2A3));
    }
}
