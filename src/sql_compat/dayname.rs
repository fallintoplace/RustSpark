use std::sync::Arc;

use chrono::Datelike;
use datafusion::{
    arrow::{array::{Date32Array, StringArray}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

/// Spark 4 returns the abbreviated English weekday name.
fn calculate(days: i32) -> Option<&'static str> {
    let day = sql_compat_spark_dates::date_from_days(days)?;
    Some(match day.weekday() {
        chrono::Weekday::Mon => "Mon",
        chrono::Weekday::Tue => "Tue",
        chrono::Weekday::Wed => "Wed",
        chrono::Weekday::Thu => "Thu",
        chrono::Weekday::Fri => "Fri",
        chrono::Weekday::Sat => "Sat",
        chrono::Weekday::Sun => "Sun",
    })
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "dayname",
        vec![DataType::Date32],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(value))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    (*value).and_then(calculate).map(str::to_owned),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("dayname expects Date32".into()))?;
            let result: StringArray = input.iter().map(|value| value.and_then(calculate)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn yields_spark_date_values_and_preserves_nulls() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new().sql("SELECT DAYNAME(d) FROM (VALUES (0, DATE '2024-02-29'), (1, DATE '2024-03-03'), (2, CAST(NULL AS DATE))) AS t(id,d) ORDER BY id")
            .await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let values = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            values.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("Thu".to_owned()), Some("Sun".to_owned()), None]);
        Ok(())
    }
}
