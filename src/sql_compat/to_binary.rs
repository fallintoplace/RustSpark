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

fn decode_hex(input: &str) -> Option<Vec<u8>> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let mut result = Vec::with_capacity(input.len() / 2);
    let mut chunks = input.as_bytes().chunks_exact(2);
    for pair in &mut chunks {
        result.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }
    if chunks.remainder().is_empty() {
        Some(result)
    } else {
        None
    }
}

fn evaluate(input0: &str, input1: &str) -> datafusion::error::Result<Option<Vec<u8>>> {
    let decoded = match input1.to_ascii_lowercase().as_str() {
        "utf8" | "utf-8" => Some(input0.as_bytes().to_vec()),
        "hex" => decode_hex(input0),
        "base64" => decode_base64(input0),
        _ => None,
    };
    decoded.map(Some).ok_or_else(|| {
        DataFusionError::Execution("TO_BINARY: invalid format or input".into())
    })
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "to_binary",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let arg0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("to_binary expects Utf8".into()))?;
            let arg1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("to_binary expects Utf8".into()))?;
            let mut result = BinaryBuilder::new();
            for row in 0..arg0.len() {
                let value = if arg0.is_null(row) || arg1.is_null(row) {
                    None
                } else {
                    evaluate(arg0.value(row), arg1.value(row))?
                };
                match value {
                    Some(value) => result.append_value(value),
                    None => result.append_null(),
                }
            }
            let result = result.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                    (!result.is_null(0)).then(|| result.value(0).to_vec()),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;

    #[test]
    fn covers_valid_invalid_and_empty_values() {
        assert_eq!(evaluate("4142", "hex").unwrap(), Some(b"AB".to_vec()));
        assert_eq!(evaluate("U3BhcmsgU1FM", "base64").unwrap(), Some(b"Spark SQL".to_vec()));
        assert_eq!(evaluate("é", "utf-8").unwrap(), Some(vec![0xC3, 0xA9]));
        assert!(evaluate("AZ", "hex").is_err());
    }
}
