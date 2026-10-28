use std::sync::Arc;

use datafusion::{
    arrow::{array::StringArray, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// RFC 3986 percent encoding of UTF-8 strings.
fn percent_encode(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let unreserved = |byte: u8| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
    };
    let Some(first_escape) = input.bytes().position(|byte| !unreserved(byte)) else {
        return input.to_owned();
    };
    let mut encoded = String::with_capacity(input.len());
    // The prefix is ASCII unreserved, so the byte offset is a UTF-8 boundary.
    encoded.push_str(&input[..first_escape]);
    for &byte in &input.as_bytes()[first_escape..] {
        if unreserved(byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0xF)]));
        }
    }
    encoded
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "url_encode",
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
                .ok_or_else(|| DataFusionError::Internal("url_encode expects Utf8".into()))?;
            let result: Vec<Option<String>> = input
                .iter()
                .map(|value| value.map(percent_encode))
                .collect();
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
    use super::percent_encode;

    #[test]
    fn copies_unreserved_prefix_and_encodes_utf8_bytes() {
        assert_eq!(percent_encode("abcXYZ-_.~"), "abcXYZ-_.~");
        assert_eq!(percent_encode("abc def"), "abc%20def");
        assert_eq!(percent_encode("café"), "caf%C3%A9");
        assert_eq!(percent_encode("a/b?c=d"), "a%2Fb%3Fc%3Dd");
        assert_eq!(percent_encode(""), "");
    }
}
