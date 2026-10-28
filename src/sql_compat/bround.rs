use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Float64Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark BROUND supports signed 64-bit bitwise conjunction.
fn evaluate(value: f64, digits: i64) -> Option<f64> {
    if !(-15..=15).contains(&digits) {
        return None;
    }
    let factor = 10_f64.powi(digits as i32);
    Some((value * factor).round_ties_even() / factor)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bround",
        vec![DataType::Float64, DataType::Int64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("bround expects Float64".into()))?;
            let input1 = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bround expects Int64".into()))?;
            let result: Float64Array = (0..input0.len())
                .map(|row| {
                    if input0.is_null(row) || input1.is_null(row) {
                        None
                    } else {
                        evaluate(input0.value(row), input1.value(row))
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
        assert_eq!(evaluate(2.5, 0), Some(2.0));
        assert_eq!(evaluate(3.5, 0), Some(4.0));
        assert_eq!(evaluate(125.0, -1), Some(120.0));
        assert_eq!(evaluate(1.5, 16), None);
    }

    #[tokio::test]
    async fn sql_function_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT BROUND(CAST(12 AS BIGINT), CAST(10 AS BIGINT))")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
