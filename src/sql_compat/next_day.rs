use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, StringArray},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(days: i32, name: &str) -> Option<i32> {
    let upper = name.to_ascii_uppercase();
    let weekday = match upper.as_str() {
        "MON" | "MONDAY" | "MO" => 0,
        "TUE" | "TUESDAY" | "TU" => 1,
        "WED" | "WEDNESDAY" | "WE" => 2,
        "THU" | "THURSDAY" | "TH" => 3,
        "FRI" | "FRIDAY" | "FR" => 4,
        "SAT" | "SATURDAY" | "SA" => 5,
        "SUN" | "SUNDAY" | "SU" => 6,
        _ => return None,
    };
    let current = (i64::from(days) + 3).rem_euclid(7);
    let mut delta = (weekday - current).rem_euclid(7);
    if delta == 0 { delta = 7; }
    days.checked_add(delta as i32)
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
                ColumnarValue::Scalar(ScalarValue::Utf8(argument)),
            ] = args {
                let result = date.as_ref().and_then(|date| {
                    argument.as_ref().and_then(|arg| value(*date, arg.as_str()))
                });
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(result)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("next_day expects Date32".into()))?;
            let arguments = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("next_day invalid second argument".into()))?;
            let result: Date32Array = dates.iter().zip(arguments.iter())
                .map(|(date, arg)| match (date, arg) {
                    (Some(date), Some(arg)) => value(date, arg),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Date32Array;

    #[test]
    fn spark_rules() {
        assert_eq!(value(0, "MON"), Some(4));
        assert_eq!(value(0, "THU"), Some(7));
        assert_eq!(value(0, "invalid"), None);
    }

    #[tokio::test]
    async fn sql_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT next_day(DATE '1970-01-01', 'MON')")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(values.value(0), 4);
        Ok(())
    }
}
