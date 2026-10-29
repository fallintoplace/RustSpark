use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
const fn crc_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0usize;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0usize;
        while bit < 8 {
            value = if value & 1 == 1 { (value >> 1) ^ 0xEDB8_8320 }
                else { value >> 1 };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = crc_table();

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    let mut crc = !0_u32;
    for byte in text.bytes() {
        crc = (crc >> 8) ^ CRC_TABLE[((crc as u8) ^ byte) as usize];
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

#[cfg(test)]
mod table_vs_bitwise_regression {
    use super::*;

    fn reference(input: &[u8]) -> u32 {
        let mut crc = !0_u32;
        for &byte in input {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            }
        }
        !crc
    }

    #[test]
    fn table_crc_matches_reference_for_variable_length_unicode_inputs() {
        for len in [0, 1, 2, 3, 7, 8, 64, 255, 256, 1_000] {
            let input = "éA".repeat(len);
            assert_eq!(
                evaluate(&[Value::Text(input.clone())]).unwrap(),
                Value::Integer(i64::from(reference(input.as_bytes())))
            );
        }
    }
}
