use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryBuilder, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    fn digit(value: u8) -> Option<u32> {
        match value {
            b'A'..=b'Z' => Some(u32::from(value - b'A')),
            b'a'..=b'z' => Some(u32::from(value - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(value - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut output = Vec::with_capacity(value.len() * 3 / 4);
    let mut buffer = 0_u32;
    let mut bits = 0_u32;
    let mut digits = 0_usize;
    let mut padding = 0_usize;
    let mut ended = false;
    for byte in value.bytes() {
        if byte.is_ascii_whitespace() {
            if ended {
                return None;
            }
            continue;
        }
        if byte == b'=' {
            ended = true;
            padding += 1;
            continue;
        }
        if ended {
            return None;
        }
        buffer = (buffer << 6) | digit(byte)?;
        bits += 6;
        digits += 1;
        if bits >= 8 {
            bits -= 8;
            output.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    if digits % 4 == 1 || padding > 2 {
        return None;
    }
    if padding > 0 && (digits + padding) % 4 != 0 {
        return None;
    }
    if padding == 1 && digits % 4 != 3 || padding == 2 && digits % 4 != 2 {
        return None;
    }
    if buffer != 0 {
        return None;
    }
    Some(output)
}

fn evaluate(value0: &str) -> datafusion::error::Result<Option<Vec<u8>>> {
    Ok(decode_base64(value0))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unbase64",
        vec![DataType::Utf8],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("unbase64 expects Utf8".into()))?;

            let mut output = BinaryBuilder::new();
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
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                    (!output.is_null(0)).then(|| output.value(0).to_vec()),
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
        assert_eq!(evaluate("U3BhcmsgU1FM").unwrap(), Some(b"Spark SQL".to_vec()));
        assert_eq!(evaluate("Zg==").unwrap(), Some(b"f".to_vec()));
        assert_eq!(evaluate("a!").unwrap(), None);
        assert_eq!(evaluate("").unwrap(), Some(Vec::new()));
    }
}
