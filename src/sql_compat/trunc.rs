use std::sync::Arc;
use datafusion::{
    arrow::{array::{Array, Date32Array, StringArray}, datatypes::DataType},
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

fn evaluate(date: i32, format: &str) -> datafusion::error::Result<Option<i32>> {
    let (year, month, _) = civil_from_days(i64::from(date));
    let first_day = match format.to_ascii_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => days_from_civil(year, 1, 1),
        "QUARTER" => days_from_civil(year, (month - 1) / 3 * 3 + 1, 1),
        "MONTH" | "MM" | "MON" => days_from_civil(year, month, 1),
        "WEEK" => i64::from(date) - (i64::from(date) + 3).rem_euclid(7),
        _ => return Ok(None),
    };
    Ok(i32::try_from(first_day).ok())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "trunc",
        vec![DataType::Date32, DataType::Utf8],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            let scalar = args.iter().all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("trunc expects Date32".into()))?;
            let format = arrays[1].as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("trunc expects Utf8".into()))?;
            let mut values = Vec::with_capacity(input.len());
            for row in 0..input.len() {
                values.push(if input.is_null(row) || format.is_null(row) {
                    None
                } else {
                    evaluate(input.value(row), format.value(row))?
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
    use super::{days_from_civil, civil_from_days, evaluate};
    #[test]
    fn truncates_by_supported_calendar_units() {
        let input = days_from_civil(2019, 8, 4) as i32;
        assert_eq!(civil_from_days(i64::from(evaluate(input, "WEEK").unwrap().unwrap())), (2019, 7, 29));
        assert_eq!(civil_from_days(i64::from(evaluate(input, "QUARTER").unwrap().unwrap())), (2019, 7, 1));
        assert_eq!(civil_from_days(i64::from(evaluate(input, "MM").unwrap().unwrap())), (2019, 8, 1));
        assert_eq!(civil_from_days(i64::from(evaluate(input, "YEAR").unwrap().unwrap())), (2019, 1, 1));
        assert_eq!(evaluate(0, "WEEK").unwrap(), Some(-3));
        assert_eq!(evaluate(input, "NOPE").unwrap(), None);
    }
}
