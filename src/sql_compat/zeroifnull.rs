use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "zeroifnull",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("zeroifnull expects Int64".into()))?;
            let result: Vec<Option<i64>> = input
                .iter()
                .map(|value| Some(value.unwrap_or(0)))
                .collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
            }
        }),
    )
}
