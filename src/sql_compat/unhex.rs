use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, BinaryBuilder, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// Decode hexadecimal text, padding an odd leading nibble with zero.
fn evaluate(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    let mut result = Vec::with_capacity(bytes.len().div_ceil(2));
    let mut index = 0;
    if bytes.len() % 2 == 1 {
        result.push(hex_digit(bytes[0])?);
        index = 1;
    }
    while index < bytes.len() {
        let high = hex_digit(bytes[index])?;
        let low = hex_digit(bytes[index + 1])?;
        result.push((high << 4) | low);
        index += 2;
    }
    Some(result)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unhex",
        vec![DataType::Utf8],
        DataType::Binary,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                    value.as_deref().and_then(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("unhex expects Utf8".into()))?;
            let mut result =
                BinaryBuilder::with_capacity(input.len(), input.value_data().len() / 2 + input.len());
            for value in input.iter() {
                match value.and_then(evaluate) {
                    Some(value) => result.append_value(value),
                    None => result.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::{Array, BinaryArray};

    #[test]
    fn hex_vectors_and_invalid_inputs() {
        assert_eq!(evaluate("4142"), Some(b"AB".to_vec()));
        assert_eq!(evaluate("ABC"), Some(vec![0x0a, 0xbc]));
        assert_eq!(evaluate("0G"), None);
        assert_eq!(evaluate(""), Some(Vec::new()));
    }

    #[tokio::test]
    async fn sql_nulls_and_invalid_hex() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT UNHEX(v) FROM (VALUES (0, '4142'), (1, CAST(NULL AS VARCHAR)), (2, 'GG')) AS t(idx, v) ORDER BY idx")
            .await?
            .collect()
            .await?;
        let column = batches[0].column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(column.value(0), b"AB");
        assert!(column.is_null(1));
        assert!(column.is_null(2));
        Ok(())
    }
}
