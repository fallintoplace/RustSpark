use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;
use chrono::Datelike;

/// Number the days of a year from January 1 = 1, including leap days.
fn calculate(days: i32) -> Option<i32> {
    let date = sql_compat_spark_dates::date_from_days(days)?;
    i32::try_from(date.ordinal()).ok()
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "dayofyear",
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
                .ok_or_else(|| DataFusionError::Internal("dayofyear expects Date32".into()))?;
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
            .sql("SELECT DAYOFYEAR(d) FROM (VALUES (0, DATE '2024-02-29'), (1, DATE '2023-12-31'), (2, DATE '2025-01-01'), (3, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            values.iter().collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some(60), Some(365), Some(1), None]);
        Ok(())
    }
}
