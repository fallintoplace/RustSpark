use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryBuilder, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(text: &str, charset: &str) -> datafusion::error::Result<Option<Vec<u8>>> {
    let encoding = charset.to_ascii_lowercase();
    let bytes = match encoding.as_str() {
        "utf-8" | "utf8" => text.as_bytes().to_vec(),
        "us-ascii" | "ascii" => text.chars()
            .map(|character| u8::try_from(character as u32).ok()
                .filter(|byte| byte.is_ascii()).unwrap_or(b'?'))
            .collect(),
        "iso-8859-1" | "latin1" | "latin-1" => text.chars()
            .map(|character| u8::try_from(character as u32).unwrap_or(b'?'))
            .collect(),
        "utf-16le" => text.encode_utf16().flat_map(u16::to_le_bytes).collect(),
        "utf-16be" => text.encode_utf16().flat_map(u16::to_be_bytes).collect(),
        "utf-16" => {
            let mut bytes = vec![0xfe, 0xff];
            bytes.extend(text.encode_utf16().flat_map(u16::to_be_bytes));
            bytes
        }
        _ => return Err(DataFusionError::Execution(
            format!("ENCODE unsupported charset: {charset}"),
        )),
    };
    Ok(Some(bytes))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "encode",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "encode expects Utf8".into(),
                ))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal(
                    "encode expects Utf8".into(),
                ))?;
            let mut output = BinaryBuilder::new();
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
    use crate::RustSparkSession;
    use datafusion::arrow::array::BinaryArray;

    #[test]
    fn reference_charsets() {
        assert_eq!(evaluate("Spark", "UTF-8").unwrap(), Some(b"Spark".to_vec()));
        assert_eq!(evaluate("é", "latin1").unwrap(), Some(vec![0xe9]));
        assert_eq!(evaluate("é", "UTF-16BE").unwrap(), Some(vec![0, 0xe9]));
        assert!(evaluate("a", "unknown").is_err());
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT ENCODE('Spark', 'UTF-8')")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), b"Spark");
        Ok(())
    }
}
