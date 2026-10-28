use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, StringArray}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(days: i32) -> Option<String> {
    Some(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][(i64::from(days) + 3).rem_euclid(7) as usize].to_owned())
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "dayname",
        vec![DataType::Date32],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(date))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(date.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("dayname expects Date32".into()))?;
            let result: StringArray = dates.iter().map(|date| date.and_then(value)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::value;
    use crate::RustSparkSession;
    use datafusion::arrow::array::{Array, StringArray};

    #[test]
    fn follows_spark_calendar_rules() {
        assert_eq!(value(0).as_deref(), Some("Thu")); assert_eq!(value(3).as_deref(), Some("Sun")); assert_eq!(value(4).as_deref(), Some("Mon"));
    }

    #[tokio::test]
    async fn sql_supports_column_and_null() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT dayname(d) FROM (VALUES (DATE '1970-01-01'), (CAST(NULL AS DATE))) AS t(d) ORDER BY d NULLS LAST")
            .await?.collect().await?;
        let actual = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(actual.value(0), "Thu");
        assert!(actual.is_null(1));
        Ok(())
    }
}
