use std::sync::Arc;

use datafusion::{
    arrow::array::{Array, StringArray, StringBuilder},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
    arrow::datatypes::DataType,
};

use crate::sql_compat_url_decode::percent_decode;

/// Spark TRY_URL_DECODE returns NULL for malformed escapes or invalid UTF-8.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_url_decode",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Utf8(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    value.as_deref().and_then(|value| percent_decode(value).ok()),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("try_url_decode expects Utf8".into())
                })?;
            let mut output = StringBuilder::new();
            for value in input.iter() {
                match value.and_then(|value| percent_decode(value).ok()) {
                    Some(decoded) => output.append_value(decoded),
                    None => output.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(output.finish())))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn converts_invalid_url_escapes_to_null() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql(
                "SELECT TRY_URL_DECODE(v) FROM (VALUES                  (0, 'spark+sql'), (1, 'caf%C3%A9'), (2, '%GG'),                  (3, '%FF'), (4, CAST(NULL AS VARCHAR)))                  AS t(i, v) ORDER BY i",
            )
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "spark sql");
        assert_eq!(values.value(1), "café");
        assert!(values.is_null(2));
        assert!(values.is_null(3));
        assert!(values.is_null(4));
        Ok(())
    }
}
