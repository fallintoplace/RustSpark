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
