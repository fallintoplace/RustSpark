use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark NEGATIVE negates a signed 64-bit integer with wrapping overflow semantics.
fn evaluate(value: i64) -> Option<i64> {
    Some(value.wrapping_neg())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "negative",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.and_then(evaluate),
                )));
            }
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("negative expects Int64".into()))?;
            let result: Int64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row))
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
        assert_eq!(evaluate(12), Some(-12));
        assert_eq!(evaluate(-5), Some(5));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT NEGATIVE(CAST(12 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), -12);
        Ok(())
    }

    #[test]
    fn negating_minimum_wraps_without_panicking() {
        assert_eq!(evaluate(i64::MIN), Some(i64::MIN));
    }
}
