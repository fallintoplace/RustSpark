use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// bit position in the 64-bit integer.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bit_get",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(left)),
                ColumnarValue::Scalar(ScalarValue::Int64(position)),
            ] = args
            {
                let value = match (*left, *position) {
                    (Some(_), Some(position)) if !(0..64).contains(&position) => {
                        return Err(DataFusionError::Execution(
                            "BIT_GET position must be between 0 and 63".into(),
                        ));
                    }
                    (Some(left), Some(position)) => {
                        Some(((left as u64 >> (position as u32)) & 1) as i64)
                    }
                    _ => None,
                };
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(value)));
            }
            // Keep a constant left argument as a scalar for the entire batch.
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Array(right),
            ] = args
            {
                let right = right
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
                let mut result = Vec::with_capacity(right.len());
                for position in right.iter() {
                    let value = match (*lhs, position) {
                        (Some(_), Some(position)) if !(0..64).contains(&position) => {
                            return Err(DataFusionError::Execution(
                                "BIT_GET position must be between 0 and 63".into(),
                            ));
                        }
                        (Some(value), Some(position)) => {
                            Some(((value as u64 >> (position as u32)) & 1) as i64)
                        }
                        _ => None,
                    };
                    result.push(value);
                }
                return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))));
            }
            // Keep constant SQL operands as scalars instead of broadcasting a new Arrow array.
            if let [
                ColumnarValue::Array(left),
                ColumnarValue::Scalar(ScalarValue::Int64(right)),
            ] = args
            {
                let left = left
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
                let mut result = Vec::with_capacity(left.len());
                for left in left.iter() {
                    result.push(match (left, *right) {
                        (Some(_), Some(right)) if !(0..64).contains(&right) => {
                            return Err(DataFusionError::Execution(
                                "BIT_GET position must be between 0 and 63".into(),
                            ));
                        }
                        (Some(left), Some(right)) => {
                            Some(((left as u64 >> (right as u32)) & 1) as i64)
                        }
                        _ => None,
                    });
                }
                return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
            let mut result = Vec::with_capacity(lhs.len());
            for (left, right) in lhs.iter().zip(rhs.iter()) {
                let value = match (left, right) {
                    (Some(_), Some(right)) if !(0..64).contains(&right) => {
                        return Err(DataFusionError::Execution(
                            "BIT_GET position must be between 0 and 63".into(),
                        ));
                    }
                    (Some(left), Some(right)) => Some(((left as u64 >> (right as u32)) & 1) as i64),
                    _ => None,
                };
                result.push(value);
            }
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
            }
        }),
    )
}

#[cfg(test)]
mod constant_left_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn evaluates_scalar_left_with_nullable_column() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT BIT_GET(CAST(5 AS BIGINT), position) FROM (VALUES (0, CAST(0 AS BIGINT)), (1, CAST(1 AS BIGINT)), (2, CAST(NULL AS BIGINT)), (3, CAST(2 AS BIGINT))) AS t(idx, position) ORDER BY idx")
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
        assert_eq!(actual, vec![Some(1), Some(0), None, Some(1)]);
        Ok(())
    }
}
