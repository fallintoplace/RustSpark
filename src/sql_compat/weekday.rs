use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn weekday(days: i32) -> i32 {
    (i64::from(days) + 3).rem_euclid(7) as i32
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "weekday",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(date))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(date.map(weekday))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("weekday expects Date32".into()))?;
            let result: Int32Array = dates.iter().map(|date| date.map(weekday)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::weekday;
    use crate::RustSparkSession;
    use datafusion::arrow::array::Int32Array;

    #[test]
    fn monday_zero_and_sunday_six_across_epoch() {
        assert_eq!(weekday(0), 3); // 1970-01-01: Thursday
        assert_eq!(weekday(4), 0); // 1970-01-05: Monday
        assert_eq!(weekday(-4), 6); // 1969-12-28: Sunday
    }

    #[tokio::test]
    async fn sql_uses_spark_weekday_numbering() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT WEEKDAY(DATE '1970-01-05'), WEEKDAY(CAST(NULL AS DATE))")
            .await?.collect().await?;
        let monday = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let missing = batches[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(monday.value(0), 0);
        assert!(missing.is_null(0));
        Ok(())
    }
}
