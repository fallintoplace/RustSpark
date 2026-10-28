use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Date32Array, Int32Array},
        datatypes::DataType,
    },
    common::{DataFusionError, ScalarValue},
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
};

fn value(days: i32, offset: i32) -> Option<i32> {
    days.checked_sub(offset)
}

pub(crate) fn udf() -> ScalarUDF {
    create_udf(
        "date_sub",
        vec![DataType::Date32, DataType::Int32],
        DataType::Date32,
        Volatility::Immutable,
        Arc::new(|args| {
            if let [
                ColumnarValue::Scalar(ScalarValue::Date32(date)),
                ColumnarValue::Scalar(ScalarValue::Int32(argument)),
            ] = args {
                let result = date.as_ref().and_then(|date| {
                    argument.as_ref().and_then(|arg| value(*date, *arg))
                });
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(result)));
            }
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let dates = arrays[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| DataFusionError::Internal("date_sub expects Date32".into()))?;
            let arguments = arrays[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("date_sub invalid second argument".into()))?;
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
        assert_eq!(value(0, 4), Some(-4));
        assert_eq!(value(0, -1), Some(1));
        assert_eq!(value(i32::MIN, 1), None);
    }

    #[tokio::test]
    async fn sql_is_registered() -> datafusion::error::Result<()> {
        let batches = RustSparkSession::new()
            .sql("SELECT date_sub(DATE '1970-01-01', CAST(4 AS INT))")
            .await?
            .collect()
            .await?;
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(values.value(0), -4);
        Ok(())
    }
}
