use super::Function;
use crate::{scalar::Value, schema::DataType, Result};

fn output(types: &[DataType]) -> Result<DataType> {
    super::text_type(types)?;
    Ok(DataType::Integer)
}
const TABLES: [[u32; 256]; 8] = {
    let mut tables = [[0_u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            bit += 1;
        }
        tables[0][index] = crc;
        index += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut index = 0;
        while index < 256 {
            let previous = tables[slice - 1][index];
            tables[slice][index] = (previous >> 8) ^ tables[0][(previous & 255) as usize];
            index += 1;
        }
        slice += 1;
    }
    tables
};

fn evaluate(args: &[Value]) -> Result<Value> {
    let Some(text) = super::string(&args[0])? else { return Ok(Value::Null) };
    // Match the bounded native digest functions. A valid SQL row must not
    // trigger an unbounded checksum scan on worker threads.
    if text.len() > 16 * 1024 * 1024 {
        return Err(crate::Error::Execution("CRC32 input exceeds native 16 MiB budget".into()));
    }
    let mut crc = !0_u32;
    let mut blocks = text.as_bytes().chunks_exact(8);
    for block in &mut blocks {
        let head = crc ^ u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        crc = TABLES[7][(head & 255) as usize]
            ^ TABLES[6][((head >> 8) & 255) as usize]
            ^ TABLES[5][((head >> 16) & 255) as usize]
            ^ TABLES[4][((head >> 24) & 255) as usize]
            ^ TABLES[3][block[4] as usize]
            ^ TABLES[2][block[5] as usize]
            ^ TABLES[1][block[6] as usize]
            ^ TABLES[0][block[7] as usize];
    }
    for &byte in blocks.remainder() {
        crc = (crc >> 8) ^ TABLES[0][((crc as u8) ^ byte) as usize];
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
            Value::Integer(0x1BE2_FA87));
    }
}

#[cfg(test)]
mod slicing_by_eight_crc_reference_tests {
    use super::*;

    fn reference(text: &str) -> u32 {
        let mut crc = !0_u32;
        for byte in text.bytes() {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 1 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            }
        }
        !crc
    }

    #[test]
    fn agrees_across_aligned_and_partial_blocks() {
        for length in 0..=128 {
            let text = "abc🙂éxyz".repeat(length);
            assert_eq!(evaluate(&[Value::Text(text.clone())]).unwrap(),
                Value::Integer(i64::from(reference(&text))));
        }
        for text in ["", "123456789", "é🙂", "abcdefgh", "abcdefghx"] {
            assert_eq!(evaluate(&[Value::Text(text.into())]).unwrap(),
                Value::Integer(i64::from(reference(text))));
        }
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}

#[cfg(test)]
mod bounded_crc32_input_regressions {
    use super::*;
    use crate::Error;

    #[test]
    fn crc32_rejects_oversized_text_before_hashing() {
        assert!(matches!(evaluate(&[
            Value::Text("x".repeat(16 * 1024 * 1024 + 1)),
        ]), Err(Error::Execution(_))));
        assert!(matches!(evaluate(&[
            Value::Text("é".repeat(8 * 1024 * 1024 + 1)),
        ]), Err(Error::Execution(_))));
        assert!(matches!(evaluate(&[
            Value::Text("x".repeat(16 * 1024 * 1024)),
        ]), Ok(Value::Integer(_))));
        assert_eq!(evaluate(&[Value::Text("123456789".into())]).unwrap(),
            Value::Integer(0xCBF4_3926));
        assert_eq!(evaluate(&[Value::Null]).unwrap(), Value::Null);
    }
}
