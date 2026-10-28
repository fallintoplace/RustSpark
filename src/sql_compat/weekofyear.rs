use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
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
fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i32> {
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
    let weekday = (i64::from(days) + 3).rem_euclid(7);
    let thursday = i32::try_from(i64::from(days) + 3 - weekday).ok()?;
    let year = civil_from_days(thursday).0;
    let january_fourth = days_from_civil(year, 1, 4)?;
    let first_monday = i64::from(january_fourth)
        - (i64::from(january_fourth) + 3).rem_euclid(7);
    Some(((i64::from(days) - first_monday).div_euclid(7) + 1) as i32)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "weekofyear",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(days))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(days.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input0 = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("weekofyear expects Date32".into()))?;
            let result: Int32Array = input0.iter().map(|a| a.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[test]
    fn matches_spark_rules() {
        assert_eq!(value(0), Some(1));
        assert_eq!(value(18628), Some(53));
        assert_eq!(value(18631), Some(1));
    }

    #[tokio::test]
    async fn accessible_in_sql() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT weekofyear(DATE '2021-01-01')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 53);
        Ok(())
    }
}
