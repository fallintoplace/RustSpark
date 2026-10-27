use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_divide",
        vec![DataType::Int64, DataType::Int64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_divide expects Int64".into()))?;
            let result: Vec<Option<f64>> = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(_), Some(0)) | (None, _) | (_, None) => None,
                    (Some(left), Some(right)) => Some(left as f64 / right as f64),
                })
                .collect();
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
