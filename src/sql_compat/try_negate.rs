use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Return NULL rather than overflow when negating Int64::MIN.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "try_negate",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.and_then(i64::checked_neg),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("try_negate expects Int64".into()))?;
            let result: Int64Array = input
                .iter()
                .map(|value| value.and_then(i64::checked_neg))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}
