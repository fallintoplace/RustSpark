use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, TimestampNanosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(unix_units: i64) -> Option<i64> {
    unix_units.checked_mul(1_000_000)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "timestamp_millis",
        vec![DataType::Int64],
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Int64(units))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                    units.and_then(value), None,
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("timestamp_millis expects Int64".into()))?;
            let result: TimestampNanosecondArray =
                input.iter().map(|units| units.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::TimestampNanosecondArray;

    #[test]
    fn converts_signed_unix_units_without_overflow() {
        assert_eq!(value(1), Some(1_000_000));
        assert_eq!(value(-1), Some(-1_000_000));
        assert_eq!(value(i64::MAX), None);
    }

    #[tokio::test]
    async fn sql_timestamp_constructor() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT timestamp_millis(CAST(1 AS BIGINT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), 1_000_000);
        Ok(())
    }
}
