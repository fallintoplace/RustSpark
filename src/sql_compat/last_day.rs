use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Date32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 { year += 1; }
    (year as i32, month as u32, day as u32)
}
fn days_in_month(year: i32, month: u32) -> u32 {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => 0,
    }
}
fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i32> {
    if day == 0 || day > days_in_month(year, month) { return None; }
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month = i64::from(month);
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = 365 * yoe + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).ok()
}
fn value(days: i32) -> Option<i32> {
    let (year, month, _) = civil_from_days(days);
    days_from_civil(year, month, days_in_month(year, month))
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "last_day",
        vec![DataType::Date32],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(date))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(date.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("last_day expects Date32".into()))?;
            let result: Date32Array = dates.iter().map(|date| date.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::{Array, Date32Array};

    #[test]
    fn follows_spark_calendar_rules() {
        assert_eq!(value(0), Some(30)); assert_eq!(value(761), Some(789));
    }

    #[tokio::test]
    async fn sql_supports_column_and_null() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT last_day(d) FROM (VALUES (DATE '1970-01-01'), (CAST(NULL AS DATE))) AS t(d) ORDER BY d NULLS LAST")
            .await?.collect().await?;
        let actual = batches[0].column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(actual.value(0), 30);
        assert!(actual.is_null(1));
        Ok(())
    }
}
