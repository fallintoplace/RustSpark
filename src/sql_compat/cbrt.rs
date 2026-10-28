use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL CBRT with Float64 coercion and NULL propagation.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "cbrt",
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
                .ok_or_else(|| DataFusionError::Internal("cbrt expects Float64".into()))?;
            let result: Vec<Option<f64>> =
                values.iter().map(|value| value.map(|x| x.cbrt())).collect();
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
