use std::sync::Arc;

use datafusion::{
    arrow::{array::StringArray, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// strict UTF-8 percent decoding with invalid-escape errors.
fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn percent_decode(input: &str) -> datafusion::error::Result<String> {
    let bytes = input.as_bytes();
    if !bytes.contains(&b'%') {
        return Ok(input.to_owned());
    }
    let mut result = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let pair = bytes.get(index + 1..index + 3).ok_or_else(|| {
                DataFusionError::Execution("Invalid URL percent-encoded byte".into())
            })?;
            let high = hex_digit(pair[0]).ok_or_else(|| {
                DataFusionError::Execution("Invalid URL percent-encoded byte".into())
            })?;
            let low = hex_digit(pair[1]).ok_or_else(|| {
                DataFusionError::Execution("Invalid URL percent-encoded byte".into())
            })?;
            result.push((high << 4) | low);
            index += 3;
        } else {
            result.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(result)
        .map_err(|_| DataFusionError::Execution("Decoded URL is not valid UTF-8".into()))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "url_decode",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("url_decode expects Utf8".into()))?;
            let mut result = Vec::with_capacity(input.len());
            for value in input.iter() {
                result.push(value.map(percent_decode).transpose()?);
            }
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(result))))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::percent_decode;

    #[test]
    fn plain_and_encoded_urls_preserve_semantics() {
        assert_eq!(percent_decode("plain/café").unwrap(), "plain/café");
        assert_eq!(percent_decode("a%20b").unwrap(), "a b");
        assert_eq!(percent_decode("%C3%A9").unwrap(), "é");
        assert!(percent_decode("%").is_err());
        assert!(percent_decode("%GG").is_err());
        assert!(percent_decode("%FF").is_err());
    }
}
