use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ACOS, including NULL and floating-point edge cases.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "acos",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("acos expects Float64".into()))?;
            let result: Vec<Option<f64>> =
                values.iter().map(|value| value.map(|x| x.acos())).collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Float64Array::from(result))))
            }
        }),
    )
}
