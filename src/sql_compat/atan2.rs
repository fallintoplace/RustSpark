use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ATAN2, including NULL and floating-point edge cases.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "atan2",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Float64(y)),
                ColumnarValue::Scalar(ScalarValue::Float64(x)),
            ] = args
            {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    (*y).zip(*x).map(|(y, x)| y.atan2(x)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let ys = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("atan2 expects Float64 y".into()))?;
            let xs = arrays[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("atan2 expects Float64 x".into()))?;
            let result: Float64Array = ys
                .iter()
                .zip(xs.iter())
                .map(|(y, x)| y.zip(x).map(|(y, x)| y.atan2(x)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}
