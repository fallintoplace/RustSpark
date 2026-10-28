use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, Time64NanosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(microseconds: i64) -> Option<i64> {
    (0..86_400_000_000).contains(&microseconds).then_some(microseconds * 1_000)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "time_from_micros",
        vec![DataType::Int64],
        DataType::Time64(TimeUnit::Nanosecond),
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(
                    ScalarValue::Time64Nanosecond(
                        value.and_then(evaluate),
                    ),
                ));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
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
        assert_eq!(evaluate(52200500000000), Some(52200500000000));
        assert_eq!(evaluate(-1), None);
    }

    #[tokio::test]
    async fn sql_scalar() -> datafusion::error::Result<()> {
        let rows = RustSparkSession::new()
            .sql("SELECT TIME_FROM_MICROS(52200500000000)")
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
