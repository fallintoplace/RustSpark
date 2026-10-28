use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

// Positive divisors do not need 128-bit widening or the normalization branch.
fn pmod_value(left: i64, right: i64) -> Option<i64> {
    if right == 0 {
        return None;
    }
    if right > 0 {
        return Some(left.rem_euclid(right));
    }
    // Preserve signed-divisor behavior, including MIN / -1.
    let divisor = i128::from(right);
    let remainder = i128::from(left) % divisor;
    let normalized = if remainder < 0 {
        (remainder + divisor) % divisor
    } else {
        remainder
    };
    i64::try_from(normalized).ok()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "pmod",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Int64(lhs)),
                ColumnarValue::Scalar(ScalarValue::Int64(rhs)),
            ] = args
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    (*lhs).zip(*rhs).and_then(|(left, right)| pmod_value(left, right)),
                )));
            }
            // A constant left argument does not need an Arrow broadcast array.
            if let [ColumnarValue::Scalar(ScalarValue::Int64(lhs)), ColumnarValue::Array(right)] = args {
                let right = right.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
                let result: Int64Array = right.iter()
                    .map(|right| (*lhs).zip(right).and_then(|(a, b)| pmod_value(a, b)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            // Reuse the scalar right argument without making a repeated Arrow array.
            if let [ColumnarValue::Array(left), ColumnarValue::Scalar(ScalarValue::Int64(rhs))] = args {
                let left = left.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
                let result: Int64Array = left.iter()
                    .map(|left| left.zip(*rhs).and_then(|(left, right)| pmod_value(left, right)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(result)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let result: Int64Array = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => pmod_value(left, right)
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::pmod_value;

    #[test]
    fn preserves_signed_divisors_and_boundaries() {
        assert_eq!(pmod_value(-5, 3), Some(1));
        assert_eq!(pmod_value(5, 3), Some(2));
        assert_eq!(pmod_value(-5, -3), Some(-2));
        assert_eq!(pmod_value(5, -3), Some(2));
        assert_eq!(pmod_value(0, 0), None);
        assert_eq!(pmod_value(i64::MIN, -1), Some(0));
        assert_eq!(pmod_value(i64::MIN, i64::MAX), Some(i64::MAX - 1));
    }
}

#[cfg(test)]
mod constant_left_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn handles_nulls_and_zero_divisors_with_scalar_left() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT PMOD(CAST(-5 AS BIGINT), v) FROM (VALUES (0, CAST(3 AS BIGINT)), (1, CAST(-3 AS BIGINT)), (2, CAST(0 AS BIGINT)), (3, CAST(NULL AS BIGINT))) AS t(idx, v) ORDER BY idx")
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let column = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            actual.extend(column.iter());
        }
        assert_eq!(actual, vec![Some(1), Some(-2), None, None]);
        Ok(())
    }
}
