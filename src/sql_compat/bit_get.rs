use std::sync::Arc;

use datafusion::{
    arrow::{array::Int64Array, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// bit position in the 64-bit integer.
pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "bit_get",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            // Keep constant SQL operands as scalars instead of broadcasting a new Arrow array.
            if let [ColumnarValue::Array(left), ColumnarValue::Scalar(ScalarValue::Int64(right))] = args {
                let left = left.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
                let mut result = Vec::with_capacity(left.len());
                for left in left.iter() {
                    result.push(match (left, *right) {
                        (Some(_), Some(right)) if !(0..64).contains(&right) => return Err(DataFusionError::Execution("BIT_GET position must be between 0 and 63".into())),
                        (Some(left), Some(right)) => Some(((left as u64 >> (right as u32)) & 1) as i64),
                        _ => None,
                    });
                }
                return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))));
            }
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let lhs = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
            let rhs = arrays[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("bit_get expects Int64".into()))?;
            let mut result = Vec::with_capacity(lhs.len());
            for (left, right) in lhs.iter().zip(rhs.iter()) {
                let value = match (left, right) {
                    (Some(_), Some(right)) if !(0..64).contains(&right) => {
                        return Err(DataFusionError::Execution(
                            "BIT_GET position must be between 0 and 63".into(),
                        ));
                    }
                    (Some(left), Some(right)) => Some(((left as u64 >> (right as u32)) & 1) as i64),
                    _ => None,
                };
                result.push(value);
            }
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
