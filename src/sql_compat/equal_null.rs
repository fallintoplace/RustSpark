use std::sync::Arc;

use datafusion::{
    arrow::{array::{BooleanArray, Int64Array}, datatypes::DataType},
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
