use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Int64Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Bucket indices: 0 below range, 1..=count within range, count+1 above.
fn bucket(value: f64, lower: f64, upper: f64, count: i64) -> datafusion::error::Result<i64> {
    if !value.is_finite()
        || !lower.is_finite()
        || !upper.is_finite()
        || lower == upper
        || count <= 0
    {
        return Err(DataFusionError::Execution(
            "WIDTH_BUCKET expects finite values, distinct bounds, and a positive bucket count"
                .into(),
        ));
    }
    let above = count.checked_add(1).ok_or_else(|| {
        DataFusionError::Execution("WIDTH_BUCKET bucket count exceeds Int64 range".into())
    })?;
    if (lower < upper && value < lower) || (lower > upper && value > lower) {
        return Ok(0);
    }
    if (lower < upper && value >= upper) || (lower > upper && value <= upper) {
        return Ok(above);
    }
    let fraction = (value - lower) / (upper - lower);
    if !fraction.is_finite() {
        return Err(DataFusionError::Execution(
            "WIDTH_BUCKET range calculation overflowed".into(),
        ));
    }
    let zero_based = (fraction * count as f64).floor() as i64;
    Ok(zero_based.clamp(0, count - 1) + 1)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "width_bucket",
        vec![
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Int64,
        ],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0].as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let lowers = arrays[1].as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let uppers = arrays[2].as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let counts = arrays[3].as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Int64".into()))?;
            let mut result = Vec::with_capacity(values.len());
            for (((value, lower), upper), count) in values
                .iter()
                .zip(lowers.iter())
                .zip(uppers.iter())
                .zip(counts.iter())
            {
                let next = match (value, lower, upper, count) {
                    (Some(value), Some(lower), Some(upper), Some(count)) => {
                        Some(bucket(value, lower, upper, count)?)
                    }
                    _ => None,
                };
                result.push(next);
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
