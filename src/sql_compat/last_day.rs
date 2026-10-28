use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Return the last valid day of the calendar month.
fn calculate(days: i32) -> Option<i32> {
    let date = sql_compat_spark_dates::date_from_days(days)?;
    let end = sql_compat_spark_dates::last_day_of_month(date)?;
    sql_compat_spark_dates::days_from_date(end)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "last_day",
        vec![DataType::Date32],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    (*value).and_then(calculate),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("last_day expects Date32".into()))?;
            let result: Date32Array = input.iter().map(|value| value.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn handles_spark_calendar_boundaries_and_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT CAST(LAST_DAY(d) AS VARCHAR) FROM (VALUES (0, DATE '2024-01-10'), (1, DATE '2024-02-01'), (2, DATE '2023-02-01'), (3, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            values.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("2024-01-31".to_owned()), Some("2024-02-29".to_owned()), Some("2023-02-28".to_owned()), None]);
        Ok(())
    }
}
