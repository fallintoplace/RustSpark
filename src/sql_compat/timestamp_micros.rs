use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, TimestampMicrosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn evaluate(value: i64) -> Option<i64> {
    value.checked_mul(1)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "timestamp_micros",
        vec![DataType::Int64],
        DataType::Timestamp(TimeUnit::Microsecond, None),
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(value))] = args {
                return Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampMicrosecond(
                        value.and_then(evaluate),
                        None,
                    ),
                ));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Int64".into()))?;
            let result: TimestampMicrosecondArray = input
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
    use datafusion::arrow::array::BooleanArray;

    #[test]
    fn epoch_and_overflow() {
        assert_eq!(evaluate(0), Some(0));
        assert_eq!(evaluate(1), Some(1));
        assert_eq!(evaluate(-1), Some(-1));
        assert_eq!(evaluate(i64::MAX), Some(i64::MAX));
    }

    #[tokio::test]
    async fn sql_scalar_and_null() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT TIMESTAMP_MICROS(1) = TIMESTAMP '1970-01-01 00:00:01'")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);

        let rows = session
            .sql("SELECT TIMESTAMP_MICROS(CAST(NULL AS BIGINT)) IS NULL")
            .await?
            .collect()
            .await?;
        let values = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(true)]);
        Ok(())
    }
}
