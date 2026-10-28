use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Float64Array, Time64NanosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() || !(0.0..86_400.0).contains(&seconds) {
        return None;
    }
    let micros = (seconds * 1_000_000.0).round() as i64;
    (micros < 86_400_000_000).then_some(micros * 1_000)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "time_from_seconds",
        vec![DataType::Float64],
        DataType::Time64(TimeUnit::Nanosecond),
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Float64(value))] = args {
                return Ok(ColumnarValue::Scalar(
                    ScalarValue::Time64Nanosecond(
                        value.and_then(evaluate),
                    ),
                ));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("Invalid TIME constructor input".into()))?;
            let result: Time64NanosecondArray = input
                .iter()
                .map(|value| value.and_then(evaluate))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Time64NanosecondArray;

    #[test]
    fn accepts_valid_times_and_rejects_invalid_values() {
        assert_eq!(evaluate(52200.5), Some(52200500000000));
        assert_eq!(evaluate(-1.0), None);
    }

    #[tokio::test]
    async fn sql_scalar() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TIME_FROM_SECONDS(CAST(52200.5 AS DOUBLE))")
            .await?
            .collect()
            .await?;
        let time = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();
        assert_eq!(time.value(0), 52200500000000);
        Ok(())
    }
}
