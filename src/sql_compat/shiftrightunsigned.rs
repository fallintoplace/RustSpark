use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// zero-fill right shift with masked shift counts.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "shiftrightunsigned",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(value)),
                ColumnarValue::Scalar(ScalarValue::Int64(shift)),
            ] = args
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    (*value)
                        .zip(*shift)
                        .map(|(value, shift)| (value as u64).wrapping_shr(shift as u32) as i64),
                )));
            }
            // Reuse the scalar value while shifting every row of the right column.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("shiftrightunsigned expects Int64".into())
                })?;
                let result: Int64Array = right
                    .iter()
                    .map(|shift| {
                        (*lhs)
                            .zip(shift)
                            .map(|(value, shift)| (value as u64).wrapping_shr(shift as u32) as i64)
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // Keep a literal right operand scalar across an entire input batch.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(right)),
            ] = args
            {
                let left = left.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("shiftrightunsigned expects Int64".into())
                })?;
                let result: Int64Array = left
                    .iter()
                    .map(|left| {
                        left.zip(*right)
                            .map(|(value, shift)| (value as u64).wrapping_shr(shift as u32) as i64)
                    })
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("shiftrightunsigned expects Int64".into())
                })?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("shiftrightunsigned expects Int64".into())
                })?;
            let result: Int64Array = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| {
                    left.zip(right)
                        .map(|(left, right)| (left as u64).wrapping_shr(right as u32) as i64)
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
    async fn shifts_scalar_value_by_nullable_column_amounts() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT SHIFTRIGHTUNSIGNED(CAST(-1 AS BIGINT), v) FROM (VALUES (0, 1), (1, 63), (2, CAST(NULL AS BIGINT)), (3, 64)) AS t(idx, v) ORDER BY idx")
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
        assert_eq!(actual, vec![Some(i64::MAX), Some(1), None, Some(-1)]);
        Ok(())
    }
}
