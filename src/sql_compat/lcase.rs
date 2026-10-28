use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL LCASE escapes single lcases with a backslash.
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
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("lcase expects Utf8".into()))?;
            let mut result =
                StringBuilder::with_capacity(input0.len(), input0.len().saturating_mul(16));
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
                    (!result.is_null(0)).then(|| result.value(0).to_owned()),
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

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate("ABC"), Some("abc".into()));
        assert_eq!(evaluate("É"), Some("é".into()));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LCASE('Spark SQL')")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
