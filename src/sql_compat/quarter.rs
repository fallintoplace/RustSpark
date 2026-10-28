use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
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
fn value(days: i32) -> Option<i32> {
    Some(((civil_from_days(days).1 - 1) / 3 + 1) as i32)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "quarter",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(date))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(date.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("quarter expects Date32".into()))?;
            let result: Int32Array = dates.iter().map(|date| date.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::{Array, Int32Array};

    #[test]
    fn follows_spark_calendar_rules() {
        assert_eq!(value(0), Some(1)); assert_eq!(value(90), Some(2)); assert_eq!(value(273), Some(4));
    }

    #[tokio::test]
    async fn sql_supports_column_and_null() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT quarter(d) FROM (VALUES (DATE '1970-01-01'), (CAST(NULL AS DATE))) AS t(d) ORDER BY d NULLS LAST")
            .await?.collect().await?;
        let actual = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(actual.value(0), 1);
        assert!(actual.is_null(1));
        Ok(())
    }
}
