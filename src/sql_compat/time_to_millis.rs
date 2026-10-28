use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, Time64NanosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Spark TIME_TO_MILLIS converts a time of day from microseconds.
fn evaluate(nanos: i64) -> Option<i64> {
    Some(nanos.div_euclid(1_000_000))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "time_to_millis",
        vec![DataType::Time64(TimeUnit::Nanosecond)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(value))] = args {
                return Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(value.and_then(evaluate)),
                ));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected TIME value".into()))?;
            let result: Int64Array = input
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
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn epoch_time_and_microseconds() {
        assert_eq!(evaluate(0), Some(0));
        assert_eq!(evaluate(52_200_000_000_000), Some(52200000));
    }

    #[tokio::test]
    async fn sql_time_value() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TIME_TO_MILLIS(make_time(14, 30, 0))")
            .await?
            .collect()
            .await?;
        let result = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.value(0), 52200000);
        Ok(())
    }
}
