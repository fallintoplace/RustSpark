use std::sync::Arc;

use datafusion::{
    arrow::{array::{StringArray, Int64Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// CRC-32/IEEE checksums of UTF-8 SQL strings.
fn checksum(input: &str) -> i64 {
    let mut crc = !0_u32;
    for byte in input.bytes() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = if crc & 1 == 0 { 0 } else { 0xEDB8_8320 };
            crc = (crc >> 1) ^ mask;
        }
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
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("crc32 expects Utf8".into()))?;
            let result: Vec<Option<i64>> = input
                .iter()
                .map(|value| value.map(checksum))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
            }
        }),
    )
}
