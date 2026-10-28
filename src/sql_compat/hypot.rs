use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Compute stable Euclidean magnitudes without overflow-prone x*x + y*y.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "hypot",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            // Keep a constant left argument as a scalar for the entire batch.
            if let [ColumnarValue::Scalar(ScalarValue::Float64(lhs)), ColumnarValue::Array(right)] = args {
                let right = right.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataFusionError::Internal("hypot expects Float64".into()))?;
                let result: Float64Array = right.iter()
                    .map(|value| (*lhs).zip(value).map(|(a, b)| a.hypot(b)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // Keep a literal right operand scalar across an entire input batch.
            if let [ColumnarValue::Array(left), ColumnarValue::Scalar(ScalarValue::Float64(right))] = args {
                let left = left.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataFusionError::Internal("hypot expects Float64".into()))?;
                let result: Vec<Option<f64>> = left.iter()
                    .map(|left| left.zip(*right).map(|(left, right)| left.hypot(right)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(Float64Array::from(result))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("hypot expects Float64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("hypot expects Float64".into()))?;
            let result: Float64Array = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| {
                    left.zip(right).map(|(left, right)| left.hypot(right))
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
mod constant_left_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Float64Array;

    #[tokio::test]
    async fn evaluates_scalar_left_with_nullable_column() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT HYPOT(CAST(3 AS DOUBLE), v) FROM (VALUES (0, CAST(4 AS DOUBLE)), (1, CAST(0 AS DOUBLE)), (2, CAST(NULL AS DOUBLE)), (3, CAST(-4 AS DOUBLE))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(5.0), Some(3.0), None, Some(5.0)]);
        Ok(())
    }
}
