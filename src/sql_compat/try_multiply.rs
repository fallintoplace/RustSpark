use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Return NULL for overflowing Int64 multiplication.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_multiply",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_multiply expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_multiply expects Int64".into()))?;
            let result: Vec<Option<i64>> = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| {
                    left.zip(right)
                        .and_then(|(left, right)| left.checked_mul(right))
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
