use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// CRC-32/IEEE checksums of UTF-8 SQL strings.
const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ 0xEDB8_8320
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

const CRC32_TABLE: [u32; 256] = make_crc32_table();

fn checksum(input: &str) -> i64 {
    let mut crc = !0_u32;
    for &byte in input.as_bytes() {
        let index = ((crc ^ u32::from(byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    i64::from(!crc)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "crc32",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.as_deref().map(checksum),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("crc32 expects Utf8".into()))?;
            let result: Int64Array = input.iter().map(|value| value.map(checksum)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::checksum;

    fn reference(input: &str) -> i64 {
        let mut crc = !0_u32;
        for byte in input.bytes() {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (if crc & 1 == 1 { 0xEDB8_8320 } else { 0 });
            }
        }
        i64::from(!crc)
    }

    #[test]
    fn table_matches_reference_and_known_crc32_vector() {
        assert_eq!(checksum(""), 0);
        assert_eq!(checksum("123456789"), 0xCBF4_3926);
        for value in ["hello", "a\0b", "café", "❄️", "A long string with spaces"] {
            assert_eq!(checksum(value), reference(value));
        }
    }
}
