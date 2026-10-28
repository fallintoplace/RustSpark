use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark LCASE converts a UTF-8 SQL string to lowercase.
fn evaluate(value: &str) -> Option<String> {
    Some(value.to_lowercase())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "lcase",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value.as_deref().and_then(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("lcase expects Utf8".into()))?;
            let mut result =
                StringBuilder::with_capacity(input0.len(), input0.len().saturating_mul(16));
            for row in 0..input0.len() {
                if input0.is_null(row) {
                    result.append_null();
                } else {
                    for character in input0.value(row).chars() {
                        for converted in character.to_lowercase() {
                            result
                                .write_char(converted)
                                .expect("writing to an Arrow string buffer cannot fail");
                        }
                    }
                    result.append_value("");
                }
            }
            let result = result.finish();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate("ABC"), Some("abc".into()));
        assert_eq!(evaluate("É"), Some("é".into()));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LCASE('RuSt')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "rust");
        Ok(())
    }
    #[tokio::test]
    async fn unicode_and_null_batches() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LCASE(v) FROM (VALUES (0, 'É'), (1, CAST(NULL AS VARCHAR)), (2, 'Rust')) AS t(idx, v) ORDER BY idx")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "é");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "rust");
        Ok(())
    }

}
