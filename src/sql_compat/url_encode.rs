use std::sync::Arc;

use datafusion::{
    arrow::{array::{Array, StringArray, StringBuilder}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark URL_ENCODE form encoding of UTF-8 strings.
fn write_encoded(output: &mut impl std::fmt::Write, input: &str) -> std::fmt::Result {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let unreserved = |byte: u8| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'*')
    };
    let Some(first_escape) = input.bytes().position(|byte| !unreserved(byte)) else {
        return output.write_str(input);
    };
    // The ASCII prefix ends on a UTF-8 boundary.
    output.write_str(&input[..first_escape])?;
    for &byte in &input.as_bytes()[first_escape..] {
        if byte == b' ' {
            output.write_char('+')?;
        } else if unreserved(byte) {
            output.write_char(char::from(byte))?;
        } else {
            output.write_char('%')?;
            output.write_char(char::from(HEX[usize::from(byte >> 4)]))?;
            output.write_char(char::from(HEX[usize::from(byte & 0xF)]))?;
        }
    }
    Ok(())
}

fn percent_encode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    write_encoded(&mut result, input).expect("writing to String cannot fail");
    result
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "url_encode",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                        value.as_deref().map(percent_encode),
                    )))
                }
                [ColumnarValue::Array(array)] => {
                    let input = array.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("url_encode expects Utf8".into()))?;
                    // Write bytes directly to Arrow's contiguous output buffer.
                    // append_value("") finalizes each string without an intermediate allocation.
                    let mut result = StringBuilder::with_capacity(
                        input.len(), input.value_data().len(),
                    );
                    for value in input.iter() {
                        if let Some(value) = value {
                            write_encoded(&mut result, value)
                                .expect("writing to an Arrow StringBuilder cannot fail");
                            result.append_value("");
                        } else {
                            result.append_null();
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(result.finish())))
                }
                _ => Err(DataFusionError::Internal("url_encode expects Utf8".into())),
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::percent_encode;

    #[test]
    fn copies_safe_prefix_and_preserves_spark_form_encoding() {
        assert_eq!(percent_encode("abcXYZ-_*."), "abcXYZ-_*.");
        assert_eq!(percent_encode("abc def"), "abc+def");
        assert_eq!(percent_encode("café"), "caf%C3%A9");
        assert_eq!(percent_encode("a/b?c=d"), "a%2Fb%3Fc%3Dd");
        assert_eq!(percent_encode("prefix~"), "prefix%7E");
        assert_eq!(percent_encode("a+b"), "a%2Bb");
        assert_eq!(percent_encode(""), "");
    }
}

#[cfg(test)]
mod array_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn encodes_strings_and_nulls_in_columnar_batches() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql(
                "SELECT URL_ENCODE(v) FROM                  (VALUES (0, 'spark sql'), (1, 'café'), (2, 'a/b'),                          (3, CAST(NULL AS VARCHAR))) AS t(idx, v) ORDER BY idx"
            )
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            actual.extend(values.iter().map(|value| value.map(str::to_owned)));
        }
        assert_eq!(actual, vec![
            Some("spark+sql".into()), Some("caf%C3%A9".into()),
            Some("a%2Fb".into()), None,
        ]);
        Ok(())
    }
}
