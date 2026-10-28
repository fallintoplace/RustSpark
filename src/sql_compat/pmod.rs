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
            // Reuse the scalar right argument without making a repeated Arrow array.
            if let [ColumnarValue::Array(left), ColumnarValue::Scalar(ScalarValue::Int64(rhs))] = args {
                let left = left.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
                let mut result = Vec::with_capacity(left.len());
                for value in left.iter() {
                    let next = match (value, *rhs) {
                        (Some(left), Some(right)) if right != 0 => {
                            let divisor = i128::from(right);
                            let remainder = i128::from(left) % divisor;
                            let normalized = if remainder < 0 { (remainder + divisor) % divisor } else { remainder };
                            i64::try_from(normalized).ok()
                        }
                        _ => None,
                    };
                    result.push(next);
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
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let result: Vec<Option<i64>> = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => pmod_value(left, right)
                    _ => None,
                })
                .collect();
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
