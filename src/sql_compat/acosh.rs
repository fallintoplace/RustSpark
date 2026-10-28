use std::sync::Arc;

use datafusion::{
    arrow::{array::Float64Array, compute::kernels::arity::unary, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark SQL ACOSH, including NULL and floating-point edge cases.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "acosh",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                    value.map(|x| x.acosh()),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("acosh expects Float64".into()))?;
            let result = unary(values, |x| x.acosh());
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}
