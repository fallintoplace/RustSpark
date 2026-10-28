use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn utf16(bytes: &[u8], little_endian: bool) -> String {
    let mut chunks = bytes.chunks_exact(2);
    let units = chunks.by_ref().map(|pair| {
        if little_endian {
            u16::from_le_bytes([pair[0], pair[1]])
        } else {
            u16::from_be_bytes([pair[0], pair[1]])
        }
    }).collect::<Vec<_>>();
    let mut result = String::from_utf16_lossy(&units);
    if !chunks.remainder().is_empty() {
        result.push('\u{fffd}');
    }
    result
}

fn evaluate(bytes: &[u8], charset: &str) -> datafusion::error::Result<Option<String>> {
    let encoding = charset.to_ascii_lowercase();
    let decoded = match encoding.as_str() {
        "utf-8" | "utf8" => String::from_utf8_lossy(bytes).into_owned(),
        "us-ascii" | "ascii" => bytes.iter()
            .map(|byte| if byte.is_ascii() { char::from(*byte) } else { '\u{fffd}' })
            .collect(),
        "iso-8859-1" | "latin1" | "latin-1" =>
            bytes.iter().copied().map(char::from).collect(),
        "utf-16le" => utf16(bytes, true),
        "utf-16be" => utf16(bytes, false),
        "utf-16" => {
            if bytes.starts_with(&[0xff, 0xfe]) {
                utf16(&bytes[2..], true)
            } else if bytes.starts_with(&[0xfe, 0xff]) {
                utf16(&bytes[2..], false)
            } else {
                utf16(bytes, false)
            }
        }
        _ => return Err(DataFusionError::Execution(
            format!("DECODE unsupported charset: {charset}"),
        )),
    };
    Ok(Some(decoded))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "decode",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "decode expects Binary".into(),
                ))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "decode expects Utf8".into(),
                ))?;
            let mut output = StringBuilder::new();
            for row in 0..input0.len() {
                let value = if input0.is_null(row) || input1.is_null(row) {
                    None
                } else {
                    evaluate(input0.value(row), input1.value(row))?
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
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn reference_charsets() {
        assert_eq!(evaluate(b"Spark", "UTF-8").unwrap(), Some("Spark".into()));
        assert_eq!(evaluate(&[0xe9], "latin1").unwrap(), Some("é".into()));
        assert_eq!(evaluate(&[0, 0xe9], "UTF-16BE").unwrap(), Some("é".into()));
        assert!(evaluate(b"a", "unknown").is_err());
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT DECODE(CAST('Spark' AS BINARY), 'UTF-8')")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "Spark");
        Ok(())
    }
}
