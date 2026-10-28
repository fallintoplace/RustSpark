use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn encode_base64(value: &[u8]) -> String {
    const DIGITS: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let a = u32::from(chunk[0]);
        let b = u32::from(*chunk.get(1).unwrap_or(&0));
        let c = u32::from(*chunk.get(2).unwrap_or(&0));
        let packed = (a << 16) | (b << 8) | c;
        output.push(DIGITS[((packed >> 18) & 63) as usize] as char);
        output.push(DIGITS[((packed >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            DIGITS[((packed >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            DIGITS[(packed & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn evaluate(value0: &[u8]) -> datafusion::error::Result<Option<String>> {
    Ok(Some(encode_base64(value0)))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "base64",
        vec![DataType::Binary],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("base64 expects Binary".into()))?;

            let mut output = StringBuilder::new();
            for row in 0..input0.len() {
                let value = if input0.is_null(row) {
                    None
                } else {
                    evaluate(input0.value(row))?
                };
                match value {
                    Some(value) => output.append_value(value),
                    None => output.append_null(),
                }
            }
            let output = output.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (!output.is_null(0)).then(|| output.value(0).to_owned()),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn matches_reference_vectors() {
        assert_eq!(evaluate(b"Spark SQL").unwrap().as_deref(), Some("U3BhcmsgU1FM"));
        assert_eq!(evaluate(b"f").unwrap().as_deref(), Some("Zg=="));
        assert_eq!(evaluate(b"fo").unwrap().as_deref(), Some("Zm8="));
        assert_eq!(evaluate(b"").unwrap().as_deref(), Some(""));
    }
}
