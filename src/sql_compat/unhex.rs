use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryBuilder, StringArray},
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
/// A reusable scratch buffer avoids allocating once per SQL input row.
fn decode_into(text: &str, output: &mut Vec<u8>) -> bool {
    output.clear();
    let bytes = text.as_bytes();
    let mut index = 0;
    if bytes.len() % 2 == 1 {
        let Some(nibble) = hex_digit(bytes[0]) else {
            return false;
        };
        output.push(nibble);
        index = 1;
    }
    while index < bytes.len() {
        let (Some(high), Some(low)) = (
            hex_digit(bytes[index]),
            hex_digit(bytes[index + 1]),
        ) else {
            return false;
        };
        output.push((high << 4) | low);
        index += 2;
    }
    true
}

fn evaluate(text: &str) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(text.len().div_ceil(2));
    decode_into(text, &mut output).then_some(output)
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
                BinaryBuilder::with_capacity(
                    input.len(),
                    input.value_data().len() / 2 + input.len(),
                );
            let mut scratch = Vec::new();
            for value in input.iter() {
                match value {
                    Some(value) if decode_into(value, &mut scratch) => {
                        result.append_value(&scratch);
                    }
                    _ => result.append_null(),
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
        assert_eq!(evaluate("f"), Some(vec![15]));
        assert_eq!(evaluate("G1"), None);
    }

    #[tokio::test]
    async fn sql_nulls_and_invalid_hex() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql(concat!(
                "SELECT UNHEX(v) FROM ",
                "(VALUES (0, '4142'), (1, CAST(NULL AS VARCHAR)), (2, 'GG')) ",
                "AS t(idx, v) ORDER BY idx"
            ))
            .await?
            .collect()
            .await?;
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(column.value(0), b"AB");
        assert!(column.is_null(1));
        assert!(column.is_null(2));
        Ok(())
    }
}
