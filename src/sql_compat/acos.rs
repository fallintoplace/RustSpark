use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
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
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| x.acos()),
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
                .ok_or_else(|| DataFusionError::Internal("acos expects Float64".into()))?;
            let result: Float64Array = unary(values, |x| x.acos());
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}
