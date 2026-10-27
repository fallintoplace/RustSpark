use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "pmod",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("pmod expects Int64".into()))?;
            let result: Vec<Option<i64>> = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) if right != 0 => {
                        let divisor = i128::from(right);
                        let remainder = i128::from(left) % divisor;
                        let normalized = if remainder < 0 {
                            (remainder + divisor) % divisor
                        } else {
                            remainder
                        };
                        i64::try_from(normalized).ok()
                    }
                    _ => None,
                })
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
