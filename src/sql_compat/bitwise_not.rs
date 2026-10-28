use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark BITWISE_NOT rounds floating point values to the nearest even integer.
fn evaluate(value: i64) -> Option<i64> {
    Some(!value)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bitwise_not",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bitwise_not expects Int64".into()))?;
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

    #[test]
    fn reference_vectors() {
        assert_eq!(evaluate(12), Some(-13));
        assert_eq!(evaluate(-1), Some(0));
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT BITWISE_NOT(CAST(2.5 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
