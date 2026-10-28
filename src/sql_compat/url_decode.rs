use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
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
        return Ok(if bytes.contains(&b'+') {
            input.replace('+', " ")
        } else {
            input.to_owned()
        });
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
            result.push(if bytes[index] == b'+' {
                b' '
            } else {
                bytes[index]
            });
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
            match args {
                [ColumnarValue::Scalar(ScalarValue::Utf8(value))] => Ok(ColumnarValue::Scalar(
                    ScalarValue::Utf8(value.as_deref().map(percent_decode).transpose()?),
                )),
                [ColumnarValue::Array(array)] => {
                    let input = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("url_decode expects Utf8".into())
                        })?;
                    let mut result =
                        StringBuilder::with_capacity(input.len(), input.value_data().len());
                    for value in input.iter() {
                        match value {
                            None => result.append_null(),
                            Some(value)
                                if !value.as_bytes().contains(&b'%')
                                    && !value.as_bytes().contains(&b'+') =>
                            {
                                // Common case: copy the existing string into Arrow once.
                                result.append_value(value);
                            }
                            Some(value) => {
                                result.append_value(percent_decode(value)?);
                            }
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(result.finish())))
                }
                _ => Err(DataFusionError::Internal("url_decode expects Utf8".into())),
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
        assert_eq!(percent_decode("a+b").unwrap(), "a b");
        assert_eq!(percent_decode("%C3%A9").unwrap(), "é");
        assert!(percent_decode("%").is_err());
        assert!(percent_decode("%GG").is_err());
        assert!(percent_decode("%FF").is_err());
    }
}

#[cfg(test)]
mod array_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn decodes_column_values_with_plain_unicode_and_nulls() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql(
                "SELECT URL_DECODE(v) FROM                  (VALUES (0, 'spark+sql'), (1, 'caf%C3%A9'),                          (2, 'plain/café'), (3, CAST(NULL AS VARCHAR))) AS t(idx, v) ORDER BY idx"
            )
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            actual.extend(values.iter().map(|value| value.map(str::to_owned)));
        }
        assert_eq!(
            actual,
            vec![
                Some("spark sql".into()),
                Some("café".into()),
                Some("plain/café".into()),
                None,
            ]
        );
        Ok(())
    }
}
