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
    let span = upper - lower;
    // Halving all terms avoids overflow when valid finite bounds straddle
    // a range larger than f64::MAX.
    let fraction = if span.is_finite() {
        (value - lower) / span
    } else {
        (value / 2.0 - lower / 2.0) / (upper / 2.0 - lower / 2.0)
    };
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
            if let [
                ColumnarValue::Scalar(ScalarValue::Float64(value)),
                ColumnarValue::Scalar(ScalarValue::Float64(lower)),
                ColumnarValue::Scalar(ScalarValue::Float64(upper)),
                ColumnarValue::Scalar(ScalarValue::Int64(count)),
            ] = args
            {
                let result = (*value)
                    .zip(*lower)
                    .zip(*upper)
                    .zip(*count)
                    .map(|(((value, lower), upper), count)| bucket(value, lower, upper, count))
                    .transpose()?;
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)));
            }
            // Do not broadcast three SQL literals into Arrow arrays for each batch.
            if let [
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(lower))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(upper))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(count))),
            ] = args
            {
                let values = values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("WIDTH_BUCKET expects Float64".into())
                    })?;
                let mut result = Vec::with_capacity(values.len());
                for value in values.iter() {
                    result.push(
                        value
                            .map(|value| bucket(value, *lower, *upper, *count))
                            .transpose()?,
                    );
                }
                return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let values = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let lowers = arrays[1]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let uppers = arrays[2]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("WIDTH_BUCKET expects Float64".into()))?;
            let counts = arrays[3]
                .as_any()
                .downcast_ref::<Int64Array>()
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
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
        }),
    )
}

#[cfg(test)]
mod constant_bounds_regression {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[tokio::test]
    async fn evaluates_constant_bounds_with_nullable_columns() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql(
                "SELECT WIDTH_BUCKET(v, CAST(0 AS DOUBLE), CAST(10 AS DOUBLE), CAST(5 AS BIGINT))                  FROM (VALUES (0, CAST(-1 AS DOUBLE)), (1, CAST(0 AS DOUBLE)),                               (2, CAST(2.5 AS DOUBLE)), (3, CAST(10 AS DOUBLE)),                               (4, CAST(NULL AS DOUBLE))) AS t(idx, v) ORDER BY idx"
            )
            .await?.collect().await?;
        let mut actual = Vec::new();
        for batch in rows {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(values.iter());
        }
        assert_eq!(actual, vec![Some(0), Some(1), Some(2), Some(6), None]);
        Ok(())
    }
}
