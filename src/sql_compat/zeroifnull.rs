use std::sync::Arc;

use datafusion::{
    arrow::{array::{Array, Int64Array}, datatypes::DataType},
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
            // ZEROIFNULL is the identity for a column without nulls.
            if let [ColumnarValue::Array(array)] = args {
                let input = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("zeroifnull expects Int64".into()))?;
                if input.null_count() == 0 {
                    return Ok(ColumnarValue::Array(Arc::clone(array)));
                }
            }
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value.unwrap_or(0)))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("zeroifnull expects Int64".into()))?;
            let result: Int64Array =
                input.iter().map(|value| value.unwrap_or(0)).collect();
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    result.iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }),
    )
}
