use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Checked Int64 addition: overflow and NULL inputs return NULL.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_add",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_add expects Int64".into()))?;
            let rhs = arrays[1].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_add expects Int64".into()))?;
            let result: Int64Array = lhs.iter().zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => left.checked_add(right),
                    _ => None,
                })
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}
