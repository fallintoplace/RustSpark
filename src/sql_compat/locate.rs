use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark LOCATE returns the one-based Unicode position of a substring.
fn evaluate(needle: &str, haystack: &str) -> Option<i64> {
    Some(haystack.find(needle).map_or(0, |offset| haystack[..offset].chars().count() as i64 + 1))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "locate",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("locate expects Utf8".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("locate expects Utf8".into()))?;
            let result: Int64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) || input1.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row), input1.value(row))
                    }
                })
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.iter().next().flatten(),
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
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate("é", "café"), Some(4));
        assert_eq!(evaluate("z", "abc"), Some(0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LOCATE('é', 'café')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 4);
        Ok(())
    }
}
