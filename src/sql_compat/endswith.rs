use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanArray, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "endswith",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            // Reuse the scalar right argument without making a repeated Arrow array.
            if let [ColumnarValue::Array(left), ColumnarValue::Scalar(ScalarValue::Utf8(rhs))] = args {
                let left = left.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
                let result: Vec<Option<bool>> = left.iter()
                    .map(|value| value.zip(rhs.as_deref()).map(|(value, suffix)| value.ends_with(suffix)))
                    .collect();
                return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
            let suffix = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("endswith expects Utf8".into()))?;
            let result: Vec<Option<bool>> = input
                .iter()
                .zip(suffix.iter())
                .map(|(value, suffix)| value.zip(suffix).map(|(s, p)| s.ends_with(p)))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))))
            }
        }),
    )
}
