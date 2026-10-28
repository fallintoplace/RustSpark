use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, BinaryArray, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Implements Spark's TRY_VALIDATE_UTF8 semantics for nullable columns.
fn evaluate(x0: &[u8]) -> Option<String> {
    std::str::from_utf8(x0).ok().map(str::to_owned)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_validate_utf8",
        vec![DataType::Binary],
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
                    "try_validate_utf8 expects Binary".into(),
                ))?;
            let mut result = StringBuilder::new();
            for row in 0..input0.len() {
                let value = if input0.is_null(row) {
                    None
                } else {
                    evaluate(input0.value(row))
                };
                match value {
                    Some(value) => result.append_value(value),
                    None => result.append_null(),
                }
            }
            let result = result.finish();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.iter().next().flatten().map(str::to_owned),
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
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn spark_reference_cases() {
        assert_eq!(evaluate(b"hello"), Some("hello".to_owned()));
        assert_eq!(evaluate(&[0xff]), None);
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TRY_VALIDATE_UTF8(CAST('hello' AS BINARY))")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "hello");
        Ok(())
    }
}
