use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Float64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark LOG1P computes ln(1 + value) precisely near zero.
fn evaluate(value: f64) -> Option<f64> {
    Some(value.ln_1p())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "log1p",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("log1p expects Float64".into()))?;
            let result: Float64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row))
                    }
                })
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(
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
        assert_eq!(evaluate(0.0), Some(0.0));
        assert!((evaluate(1e-10).unwrap() - 1e-10).abs() < 1e-18);
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT LOG1P(CAST(1 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
