use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Return NULL for overflowing Int64 subtraction.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_subtract",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            // Constant expressions do not need to become one-row Arrow arrays.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Scalar(ScalarValue::Int64(rhs)),
            ] = args
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    (*lhs)
                        .zip(*rhs)
                        .and_then(|(left, right)| left.checked_sub(right)),
                )));
            }
            // A constant left argument does not need an Arrow broadcast array.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("try_subtract expects Int64".into())
                })?;
                let result: Int64Array = right
                    .iter()
                    .map(|right| (*lhs).zip(right).and_then(|(a, b)| a.checked_sub(b)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // A SQL literal remains a scalar. Avoid expanding it into a full input array.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(rhs)),
            ] = args
            {
                let left = left.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("try_subtract expects Int64".into())
                })?;
                let result: Int64Array = left
                    .iter()
                    .map(|left| {
                        left.zip(*rhs)
                            .and_then(|(left, right)| left.checked_sub(right))
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_subtract expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_subtract expects Int64".into()))?;
            let result: Int64Array = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| {
                    left.zip(right)
                        .and_then(|(left, right)| left.checked_sub(right))
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod constant_left_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn handles_nulls_and_boundary_values_with_scalar_left() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TRY_SUBTRACT(CAST(10 AS BIGINT), v) FROM (VALUES (0, CAST(2 AS BIGINT)), (1, CAST(-1 AS BIGINT)), (2, CAST(NULL AS BIGINT)), (3, CAST('-9223372036854775808' AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(8), Some(11), None, None]);
        Ok(())
    }
}
