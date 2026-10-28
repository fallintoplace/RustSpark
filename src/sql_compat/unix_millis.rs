use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, TimestampNanosecondArray},
        datatypes::{DataType, TimeUnit},
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(nanoseconds: i64) -> i64 {
    nanoseconds.div_euclid(1_000_000)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "unix_millis",
        vec![DataType::Timestamp(TimeUnit::Nanosecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(value, _))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(value.map(value_fn))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| DataFusionError::Internal("unix_millis expects timestamp ns".into()))?;
            let result: Int64Array = input.iter().map(|n| n.map(value_fn)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

fn value_fn(nanoseconds: i64) -> i64 {
    value(nanoseconds)
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn floors_negative_epoch_values() {
        assert_eq!(value(0), 0);
        assert_eq!(value(-1), -1);
        assert_eq!(value(1_123_456_789), 1123);
    }

    #[tokio::test]
    async fn sql_timestamp_conversion() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT unix_millis(TIMESTAMP '1970-01-01 00:00:01.123456789')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1123);
        Ok(())
    }
}
