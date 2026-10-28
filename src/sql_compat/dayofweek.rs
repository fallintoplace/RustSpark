use std::sync::Arc;

use datafusion::{
    arrow::{array::{Date32Array, Int32Array}, datatypes::DataType},
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(days: i32) -> Option<i32> {
    Some(((i64::from(days) + 4).rem_euclid(7) + 1) as i32)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "dayofweek",
        vec![DataType::Date32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [ColumnarValue::Scalar(ScalarValue::Date32(date))] = args {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int32(date.and_then(value))));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0].as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("dayofweek expects Date32".into()))?;
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
        assert_eq!(value(0), Some(5)); assert_eq!(value(3), Some(1)); assert_eq!(value(-4), Some(1));
    }

    #[tokio::test]
    async fn sql_supports_column_and_null() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT dayofweek(d) FROM (VALUES (DATE '1970-01-01'), (CAST(NULL AS DATE))) AS t(d) ORDER BY d NULLS LAST")
            .await?.collect().await?;
        let actual = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(actual.value(0), 5);
        assert!(actual.is_null(1));
        Ok(())
    }
}
