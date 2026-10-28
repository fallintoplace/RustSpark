use std::sync::Arc;

use chrono::{Datelike, Duration, Weekday};
use datafusion::{
    arrow::{array::{Date32Array, StringArray}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

use crate::sql_compat_spark_dates;

fn weekday(name: &str) -> Option<Weekday> {
    match name.trim().to_ascii_uppercase().as_str() {
        "MO" | "MON" | "MONDAY" => Some(Weekday::Mon),
        "TU" | "TUE" | "TUESDAY" => Some(Weekday::Tue),
        "WE" | "WED" | "WEDNESDAY" => Some(Weekday::Wed),
        "TH" | "THU" | "THURSDAY" => Some(Weekday::Thu),
        "FR" | "FRI" | "FRIDAY" => Some(Weekday::Fri),
        "SA" | "SAT" | "SATURDAY" => Some(Weekday::Sat),
        "SU" | "SUN" | "SUNDAY" => Some(Weekday::Sun),
        _ => None,
    }
}

fn calculate(days: i32, target: &str) -> Option<i32> {
    let date = sql_compat_spark_dates::date_from_days(days)?;
    let target = weekday(target)?.num_days_from_monday();
    let current = date.weekday().num_days_from_monday();
    let mut delta = (target + 7 - current) % 7;
    if delta == 0 {
        delta = 7;
    }
    let next = date.checked_add_signed(Duration::days(i64::from(delta)))?;
    sql_compat_spark_dates::days_from_date(next)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "next_day",
        vec![DataType::Date32, DataType::Utf8],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Date32(date)),
                ColumnarValue::Scalar(ScalarValue::Utf8(weekday)),
            ] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                    (*date).zip(weekday.as_deref()).and_then(|(d,w)| calculate(d,w)),
                )));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("next_day expects Date32".into()))?;
            let weekdays = arrays[1].as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("next_day expects Utf8".into()))?;
            let result: Date32Array = dates.iter().zip(weekdays.iter())
                .map(|(d,w)| d.zip(w).and_then(|(d,w)| calculate(d,w)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use crate::RustSparkSession;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn next_day_is_strictly_after_input_date() -> datafusion::error::Result<()> {
        let query = "SELECT CAST(NEXT_DAY(d,w) AS VARCHAR) FROM (VALUES (0, DATE '2024-02-26', 'Mon'), (1, DATE '2024-02-27', 'Wed'), (2, DATE '2024-02-27', 'bad'), (3, CAST(NULL AS DATE), 'Mon')) AS t(id,d,w) ORDER BY id";
        let batches = RustSparkSession::new().sql(query).await?.collect().await?;
        let actual: Vec<_> = batches.iter().flat_map(|batch| {
            let v = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            v.iter().map(|v| v.map(str::to_owned)).collect::<Vec<_>>()
        }).collect();
        assert_eq!(actual, vec![Some("2024-03-04".to_owned()), Some("2024-02-28".to_owned()), None, None]);
        Ok(())
    }
}
