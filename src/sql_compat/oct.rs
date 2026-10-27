use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "oct",
        vec![DataType::Int64],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("oct expects Int64".into()))?;
            let result: Vec<Option<String>> = input
                .iter()
                .map(|value| value.map(|number| format!("{:o}", number as u64)))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(result))))
            }
        }),
    )
}
