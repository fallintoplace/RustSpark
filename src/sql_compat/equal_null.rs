use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// NULL-safe Int64 equality.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "equal_null",
        vec![DataType::Int64, DataType::Int64],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            // Keep a constant left argument as a scalar for the entire batch.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("equal_null expects Int64".into()))?;
                let result: BooleanArray = right.iter().map(|value| *lhs == value).collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // Keep a literal right operand scalar across an entire input batch.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(right)),
            ] = args
            {
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("equal_null expects Int64".into()))?;
                let result: BooleanArray = left.iter().map(|left| left == *right).collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("equal_null expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("equal_null expects Int64".into()))?;
            let result: BooleanArray = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| left == right)
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
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
    use datafusion::arrow::array::BooleanArray;

    #[tokio::test]
    async fn evaluates_scalar_left_with_nullable_column() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT EQUAL_NULL(CAST(3 AS BIGINT), v) FROM (VALUES (0, CAST(3 AS BIGINT)), (1, CAST(NULL AS BIGINT)), (2, CAST(4 AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }
}
