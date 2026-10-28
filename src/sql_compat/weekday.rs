use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;
use chrono::Datelike;

/// Spark WEEKDAY starts on Monday=0 and ends Sunday=6.
fn calculate(days: i32) -> Option<i32> {
    let weekday = sql_compat_spark_dates::date_from_days(days)?.weekday();
    i32::try_from(weekday.num_days_from_monday()).ok()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "weekday",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    (*value).and_then(calculate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("weekday expects Date32".into()))?;
            let result: Int32Array = input.iter().map(|value| value.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[tokio::test]
    async fn respects_spark_calendar_values_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT WEEKDAY(d) FROM (VALUES (0, DATE '1970-01-01'), (1, DATE '2024-02-26'), (2, DATE '2024-03-03'), (3, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            values.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some(3), Some(0), Some(6), None]);
        Ok(())
    }
}
