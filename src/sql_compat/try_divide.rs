use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_divide",
        vec![DataType::Int64, DataType::Int64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            // A constant left argument does not need an Arrow broadcast array.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
                let result: Float64Array = right
                    .iter()
                    .map(|right| {
                        (*lhs)
                            .zip(right)
                            .and_then(|(a, b)| (b != 0).then(|| a as f64 / b as f64))
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // A SQL literal remains a scalar. Avoid expanding it into a full input array.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(rhs)),
            ] = args
            {
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
                let result: Vec<Option<f64>> = left
                    .iter()
                    .map(|left| {
                        left.zip(*rhs).and_then(|(left, right)| {
                            (right != 0).then(|| left as f64 / right as f64)
                        })
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(Float64Array::from(result))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
            let result: Float64Array = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(_), Some(0)) | (None, _) | (_, None) => None,
                    (Some(left), Some(right)) => Some(left as f64 / right as f64),
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
    async fn handles_nulls_and_zero_divisors_with_scalar_left() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TRY_DIVIDE(CAST(10 AS BIGINT), v) FROM (VALUES (0, CAST(2 AS BIGINT)), (1, CAST(-4 AS BIGINT)), (2, CAST(0 AS BIGINT)), (3, CAST(NULL AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(5.0), Some(-2.5), None, None]);
        Ok(())
    }
}
