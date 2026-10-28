use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL SIN with Float64 coercion and NULL propagation.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "sin",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| x.sin()),
                )));
            }
            let owned_arrays;
            let array = match args {
                [ColumnarValue::Array(array)] => array,
                _ => {
                    owned_arrays = ColumnarValue::values_to_arrays(args)?;
                    &owned_arrays[0]
                }
            };
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("sin expects Float64".into()))?;
            let result: Float64Array = unary(values, |x| x.sin());
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}
