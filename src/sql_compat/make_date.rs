use std::sync::Arc;
use datafusion::{
    arrow::{array::{Array, Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719468;
    let era = shifted.div_euclid(146097);
    let doe = shifted - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    (year + i64::from(month <= 2), month, day)
}

fn is_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn evaluate(year: i32, month: i32, day: i32) -> datafusion::error::Result<Option<i32>> {
    let last_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    };
    if !(1..=9999).contains(&year) || !(1..=last_day).contains(&day) {
        return Err(DataFusionError::Execution("MAKE_DATE: invalid calendar date".into()));
    }
    let epoch_days = days_from_civil(i64::from(year), i64::from(month), i64::from(day));
    Ok(i32::try_from(epoch_days).ok())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "make_date",
        vec![DataType::Int32, DataType::Int32, DataType::Int32],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let year = arrays[0].as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("make_date expects Int32 year".into()))?;
            let month = arrays[1].as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("make_date expects Int32 month".into()))?;
            let day = arrays[2].as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("make_date expects Int32 day".into()))?;
            let mut values = Vec::with_capacity(year.len());
            for row in 0..year.len() {
                values.push(if year.is_null(row) || month.is_null(row) || day.is_null(row) {
                    None
                } else {
                    evaluate(year.value(row), month.value(row), day.value(row))?
                });
            }
            if scalar {
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(values.into_iter().next().flatten())))
            } else {
                Ok(ColumnarValue::Array(Arc::new(Date32Array::from(values))))
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    #[test]
    fn handles_epoch_leap_year_and_invalid_dates() {
        assert_eq!(evaluate(1970, 1, 1).unwrap(), Some(0));
        assert_eq!(evaluate(1969, 12, 31).unwrap(), Some(-1));
        assert_eq!(evaluate(2024, 2, 29).unwrap(), Some(19782));
        assert!(evaluate(2023, 2, 29).is_err());
        assert!(evaluate(2024, 0, 1).is_err());
        assert!(evaluate(0, 1, 1).is_err());
    }
}
