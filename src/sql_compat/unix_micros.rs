use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, TimestampMicrosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

/// Microseconds to Spark UNIX_MICROS with floor semantics for pre-epoch timestamps.
fn evaluate(micros: i64) -> i64 {
    micros.div_euclid(1)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unix_micros",
        vec![DataType::Timestamp(TimeUnit::Microsecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, _))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    value.map(evaluate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected microsecond timestamp".into()))?;
            let result: Int64Array = input
                .iter()
                .map(|value| value.map(evaluate))
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
    fn epoch_and_negative_fraction() {
        assert_eq!(evaluate(0), 0);
        assert_eq!(evaluate(1), 1);
        assert_eq!(evaluate(-1), -1);
    }

    #[tokio::test]
    async fn sql_scalar_and_null() -> datafusion::error::Result<()> {
        let session = RustSparkSession::new();
        let rows = session
            .sql("SELECT UNIX_MICROS(TIMESTAMP '1970-01-01 00:00:01') = 1000000")
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
            .sql("SELECT UNIX_MICROS(CAST(NULL AS TIMESTAMP)) IS NULL")
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
